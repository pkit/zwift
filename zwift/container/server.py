import os
import cPickle as pickle
from xml.etree.cElementTree import Element, SubElement, tostring
from datetime import datetime
import sqlite3
import errno
from swift.common.db import utf8encodekeys, DatabaseConnectionError, \
    PENDING_CAP, PICKLE_PROTOCOL, utf8encode
from swift.common.db_replicator import ReplicatorRpc

from swift.common.request_helpers import is_sys_or_user_meta, \
    is_user_meta, get_param
from swift.common.swob import Response, HTTPNoContent
from swift.container.backend import ContainerBroker
from swift.container.server import ContainerController
from swift.common.utils import json, override_bytes_from_content_type, \
    TRUE_VALUES, hash_path, storage_directory, lock_parent_directory

DATADIR = 'metacontainers'
OLD_DATADIR = 'containers'


class ContainerMetaBroker(ContainerBroker):

    def create_object_table(self, conn):
        """
        Create the object table which is specific to the container DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.

        :param conn: DB connection object
        """
        conn.executescript("""
            CREATE TABLE object (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                created_at TEXT,
                size INTEGER,
                content_type TEXT,
                etag TEXT,
                deleted INTEGER DEFAULT 0,
                metadata TEXT DEFAULT ''
            );

            CREATE INDEX ix_object_deleted_name ON object (deleted, name);

            CREATE TRIGGER object_insert AFTER INSERT ON object
            BEGIN
                UPDATE container_stat
                SET object_count = object_count + (1 - new.deleted),
                    bytes_used = bytes_used + new.size,
                    hash = chexor(hash, new.name, new.created_at);
            END;

            CREATE TRIGGER object_update BEFORE UPDATE ON object
            BEGIN
                SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
            END;

            CREATE TRIGGER object_delete AFTER DELETE ON object
            BEGIN
                UPDATE container_stat
                SET object_count = object_count - (1 - old.deleted),
                    bytes_used = bytes_used - old.size,
                    hash = chexor(hash, old.name, old.created_at);
            END;
        """)

    def _commit_puts_load(self, item_list, entry):
        """See :func:`swift.common.db.DatabaseBroker._commit_puts_load`"""
        (name, timestamp, size, content_type, etag, deleted, metadata) = \
            pickle.loads(entry.decode('base64'))
        item_list.append({'name': name,
                          'created_at': timestamp,
                          'size': size,
                          'content_type': content_type,
                          'etag': etag,
                          'metadata': metadata,
                          'deleted': deleted})

    def put_object(self, name, timestamp, size, content_type, etag,
                   deleted=0, metadata=None):
        """
        Creates an object in the DB with its metadata.

        :param name: object name to be created
        :param timestamp: timestamp of when the object was created
        :param size: object size
        :param content_type: object content-type
        :param etag: object etag
        :param deleted: if True, marks the object as deleted and sets the
                        deteleted_at timestamp to timestamp
        """
        record = {'name': name, 'created_at': timestamp, 'size': size,
                  'content_type': content_type, 'etag': etag,
                  'metadata': metadata, 'deleted': deleted}
        if self.db_file == ':memory:':
            self.merge_items([record])
            return
        if not os.path.exists(self.db_file):
            raise DatabaseConnectionError(self.db_file, "DB doesn't exist")
        pending_size = 0
        try:
            pending_size = os.path.getsize(self.pending_file)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
        if pending_size > PENDING_CAP:
            self._commit_puts([record])
        else:
            with lock_parent_directory(self.pending_file,
                                       self.pending_timeout):
                with open(self.pending_file, 'a+b') as fp:
                    # Colons aren't used in base64 encoding; so they are our
                    # delimiter
                    fp.write(':')
                    fp.write(pickle.dumps(
                        (name, timestamp, size, content_type, etag, deleted,
                         metadata),
                        protocol=PICKLE_PROTOCOL).encode('base64'))
                    fp.flush()

    def delete_object(self, name, timestamp):
        """
        Mark an object deleted.

        :param name: object name to be deleted
        :param timestamp: timestamp when the object was marked as deleted
        """
        self.put_object(name, timestamp, 0, 'application/deleted', 'noetag',
                        deleted=1, metadata='{}')

    def list_objects_iter(self, limit, marker, end_marker, prefix, delimiter,
                          path=None):
        """
        Get a list of objects sorted by name starting at marker onward, up
        to limit entries.  Entries will begin with the prefix and will not
        have the delimiter after the prefix.

        :param limit: maximum number of entries to get
        :param marker: marker query
        :param end_marker: end marker query
        :param prefix: prefix query
        :param delimiter: delimiter for query
        :param path: if defined, will set the prefix and delimter based on
                     the path

        :returns: list of tuples of (name, created_at, size, content_type,
                  etag)
        """
        delim_force_gte = False
        (marker, end_marker, prefix, delimiter, path) = utf8encode(
            marker, end_marker, prefix, delimiter, path)
        self._commit_puts_stale_ok()
        if path is not None:
            prefix = path
            if path:
                prefix = path = path.rstrip('/') + '/'
            delimiter = '/'
        elif delimiter and not prefix:
            prefix = ''
        orig_marker = marker
        with self.get() as conn:
            results = []
            while len(results) < limit:
                query_meta = '''SELECT name, created_at, size, content_type,
                                etag, metadata FROM object WHERE'''
                query_old = '''SELECT name, created_at, size, content_type,
                               etag, '{}' FROM object WHERE'''
                query = ''
                query_args = []
                if end_marker:
                    query += ' name < ? AND'
                    query_args.append(end_marker)
                if delim_force_gte:
                    query += ' name >= ? AND'
                    query_args.append(marker)
                    # Always set back to False
                    delim_force_gte = False
                elif marker and marker >= prefix:
                    query += ' name > ? AND'
                    query_args.append(marker)
                elif prefix:
                    query += ' name >= ? AND'
                    query_args.append(prefix)
                if self.get_db_version(conn) < 1:
                    query += ' +deleted = 0'
                else:
                    query += ' deleted = 0'
                query += ' ORDER BY name LIMIT ?'
                query_args.append(limit - len(results))
                try:
                    curs = conn.execute(query_meta + query, query_args)
                except sqlite3.OperationalError as err:
                    if 'no such column: metadata' \
                            not in str(err):
                        raise
                    curs = conn.execute(query_old + query, query_args)
                curs.row_factory = None

                if prefix is None:
                    # A delimiter without a specified prefix is ignored
                    return [r for r in curs]
                if not delimiter:
                    if not prefix:
                        # It is possible to have a delimiter but no prefix
                        # specified. As above, the prefix will be set to the
                        # empty string, so avoid performing the extra work to
                        # check against an empty prefix.
                        return [r for r in curs]
                    else:
                        return [r for r in curs if r[0].startswith(prefix)]

                # We have a delimiter and a prefix (possibly empty string) to
                # handle
                rowcount = 0
                for row in curs:
                    rowcount += 1
                    marker = name = row[0]
                    if len(results) >= limit or not name.startswith(prefix):
                        curs.close()
                        return results
                    end = name.find(delimiter, len(prefix))
                    if path is not None:
                        if name == path:
                            continue
                        if end >= 0 and len(name) > end + len(delimiter):
                            marker = name[:end] + chr(ord(delimiter) + 1)
                            curs.close()
                            break
                    elif end > 0:
                        marker = name[:end] + chr(ord(delimiter) + 1)
                        # we want result to be inclusinve of delim+1
                        delim_force_gte = True
                        dir_name = name[:end + 1]
                        if dir_name != orig_marker:
                            results.append([dir_name, '0', 0, None, '', '{}'])
                        curs.close()
                        break
                    results.append(row)
                if not rowcount:
                    break
            return results

    def _insert_object(self, conn, rec):
        conn.execute('''
            INSERT INTO object (name, created_at, size, content_type,
                                etag, deleted, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', ([rec['name'], rec['created_at'], rec['size'],
               rec['content_type'], rec['etag'],
               rec['deleted'], rec['metadata']]))

    def merge_items(self, item_list, source=None):
        """
        Merge items into the object table.

        :param item_list: list of dictionaries of {'name', 'created_at',
                          'size', 'content_type', 'etag', 'deleted'}
        :param source: if defined, update incoming_sync with the source
        """
        with self.get() as conn:
            max_rowid = -1
            for rec in item_list:
                query = '''
                    DELETE FROM object
                    WHERE name = ? AND (created_at < ?)
                '''
                if self.get_db_version(conn) >= 1:
                    query += ' AND deleted IN (0, 1)'
                conn.execute(query, (rec['name'], rec['created_at']))
                query = 'SELECT 1 FROM object WHERE name = ?'
                if self.get_db_version(conn) >= 1:
                    query += ' AND deleted IN (0, 1)'
                if not conn.execute(query, (rec['name'],)).fetchall():
                    try:
                        self._insert_object(conn, rec)
                    except sqlite3.OperationalError as err:
                        if 'table object has no column named metadata' \
                                not in str(err):
                            raise
                        conn.execute('''
                            ALTER TABLE object
                            ADD COLUMN metadata TEXT DEFAULT ''
                        ''')
                        self._insert_object(conn, rec)
                if source:
                    max_rowid = max(max_rowid, rec['ROWID'])
            if source:
                try:
                    conn.execute('''
                        INSERT INTO incoming_sync (sync_point, remote_id)
                        VALUES (?, ?)
                    ''', (max_rowid, source))
                except sqlite3.IntegrityError:
                    conn.execute('''
                        UPDATE incoming_sync SET sync_point=max(?, sync_point)
                        WHERE remote_id=?
                    ''', (max_rowid, source))
            conn.commit()


class ContainerMetaController(ContainerController):

    def __init__(self, conf, logger=None):
        super(ContainerMetaController, self).__init__(conf)
        self.replicator_rpc = ReplicatorRpc(
            self.root, DATADIR, ContainerMetaBroker, self.mount_check,
            logger=self.logger)

    def _get_container_broker(self, drive, part, account, container, **kwargs):
        """
        Get a DB broker for the container.

        :param drive: drive that holds the container
        :param part: partition the container is in
        :param account: account name
        :param container: container name
        :returns: ContainerBroker object
        """
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        kwargs.setdefault('account', account)
        kwargs.setdefault('container', container)
        kwargs.setdefault('logger', self.logger)
        return ContainerMetaBroker(db_path, **kwargs)

    def update_data_record(self, record, list_meta=False):
        """
        Perform any mutations to container listing records that are common to
        all serialization formats, and returns it as a dict.

        Converts created time to iso timestamp.
        Replaces size with 'swift_bytes' content type parameter.

        :params record: object entry record
        :returns: modified record
        """
        (name, created, size, content_type, etag, metadata) = record
        if content_type is None:
            return {'subdir': name}
        response = {'bytes': size, 'hash': etag, 'name': name,
                    'content_type': content_type}
        if list_meta:
            metadata = json.loads(metadata)
            utf8encodekeys(metadata)
            response['metadata'] = metadata
        last_modified = datetime.utcfromtimestamp(float(created)).isoformat()
        # python isoformat() doesn't include msecs when zero
        if len(last_modified) < len("1970-01-01T00:00:00.000000"):
            last_modified += ".000000"
        response['last_modified'] = last_modified
        override_bytes_from_content_type(response, logger=self.logger)
        return response

    def create_listing(self, req, out_content_type, info, metadata,
                       container_list, container):
        list_meta = get_param(req, 'list_meta', 'f').lower() in TRUE_VALUES
        resp_headers = {
            'X-Container-Object-Count': info['object_count'],
            'X-Container-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp'],
        }
        for key, (value, timestamp) in metadata.iteritems():
            if value and (key.lower() in self.save_headers or
                          is_sys_or_user_meta('container', key)):
                resp_headers[key] = value
        ret = Response(request=req, headers=resp_headers,
                       content_type=out_content_type, charset='utf-8')
        if out_content_type == 'application/json':
            ret.body = json.dumps([self.update_data_record(record, list_meta)
                                   for record in container_list])
        elif out_content_type.endswith('/xml'):
            doc = Element('container', name=container.decode('utf-8'))
            for obj in container_list:
                record = self.update_data_record(obj, list_meta)
                if 'subdir' in record:
                    name = record['subdir'].decode('utf-8')
                    sub = SubElement(doc, 'subdir', name=name)
                    SubElement(sub, 'name').text = name
                else:
                    obj_element = SubElement(doc, 'object')
                    for field in ["name", "hash", "bytes", "content_type",
                                  "last_modified"]:
                        SubElement(obj_element, field).text = str(
                            record.pop(field)).decode('utf-8')
                    for field in sorted(record):
                        if list_meta and field == 'metadata':
                            meta = SubElement(obj_element, field)
                            for k, v in record[field].iteritems():
                                SubElement(meta, k).text = str(
                                    v.decode('utf-8'))
                        else:
                            SubElement(obj_element, field).text = str(
                                record[field]).decode('utf-8')
            ret.body = tostring(doc, encoding='UTF-8').replace(
                "<?xml version='1.0' encoding='UTF-8'?>",
                '<?xml version="1.0" encoding="UTF-8"?>', 1)
        else:
            if not container_list:
                return HTTPNoContent(request=req, headers=resp_headers)
            ret.body = '\n'.join(rec[0] for rec in container_list) + '\n'
        return ret

    def object_update(self, req, broker, name, timestamp):
        metadata = json.dumps(dict([val for val in req.headers.iteritems()
                                    if is_user_meta('object', val[0])]))
        broker.put_object(name, timestamp, int(req.headers['x-size']),
                          req.headers['x-content-type'],
                          req.headers['x-etag'],
                          metadata=metadata)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ContainerMetaController(conf)
