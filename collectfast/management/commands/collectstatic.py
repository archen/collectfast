# -*- coding: utf-8 -*-

from __future__ import with_statement

import hashlib
from optparse import make_option
import datetime

from django.contrib.staticfiles.management.commands import collectstatic
from django.core.cache import cache


class Command(collectstatic.Command):
    option_list = collectstatic.Command.option_list + (
        make_option('--ignore-etag',
            action="store_true", dest="ignore_etag", default=False,
            help="Upload the file even though the ETags match."),
    )

    lookups = None

    def set_options(self, **options):
        self.ignore_etag = options.pop('ignore_etag', False)
        super(Command, self).set_options(**options)

    def collect(self, *args, **kwargs):
        """Override collect method to track time"""

        self.num_skipped_files = 0
        start = datetime.datetime.now()
        ret = super(Command, self).collect(*args, **kwargs)
        self.collect_time = str(datetime.datetime.now() - start)
        return ret

    def get_cache_key(self, path):
        # Python 2/3 support for path hashing
        try:
            return 'collectfast_asset_' + hashlib.md5(path).hexdigest()
        except TypeError:
            return 'collectfast_asset_' + hashlib.md5(path.encode('utf-8')).hexdigest()

    def get_lookup(self, path):
        """Get lookup from local dict, cache or S3 — in that order"""

        if self.lookups is None:
            self.lookups = {}

        if path not in self.lookups:
            cache_key = self.get_cache_key(path)
            cached = cache.get(cache_key, False)

            if cached is False:
                self.lookups[path] = self.storage.bucket.lookup(path)
                cache.set(cache_key, self.lookups[path])
            else:
                self.lookups[path] = cached

        return self.lookups[path]

    def destroy_lookup(self, path):
        if path in self.lookups:
            del self.lookups[path]
        cache.delete(self.get_cache_key(path))

    def copy_file(self, path, prefixed_path, source_storage):
        """
        Attempt to generate an md5 hash of the local file and compare it with
        the S3 version's ETag before copying the file.

        """

        if not self.ignore_etag and not self.dry_run:
            try:
                storage_lookup = self.get_lookup(prefixed_path)
                local_file = source_storage.open(prefixed_path)

                # Create md5 checksum from local file
                file_contents = local_file.read()
                try:
                    local_etag = '"%s"' % hashlib.md5(file_contents).hexdigest()
                except TypeError:
                    local_etag = '"%s"' % hashlib.md5(file_contents.encode('utf-8')).hexdigest()

                # Compare checksums and skip copying if matching
                if storage_lookup.etag == local_etag:
                    self.log(u"Skipping '%s' based on matching ETags" % path,
                             level=2)
                    self.num_skipped_files += 1
                    return False
                else:
                    self.log(u"ETag didn't match", level=2)
            except:
                # Ignore errors, let default Command handle it
                pass

            # Invalidate cached versions of lookup if copy is done
            self.destroy_lookup(prefixed_path)

        return super(Command, self).copy_file(path, prefixed_path,
                                              source_storage)

    def delete_file(self, path, prefixed_path, source_storage):
        """Override delete_file to skip modified time and exists lookups"""
        if self.dry_run:
            self.log(u"Pretending to delete '%s'" % path)
        else:
            self.log(u"Deleting '%s'" % path)
            self.storage.delete(prefixed_path)
        return True
