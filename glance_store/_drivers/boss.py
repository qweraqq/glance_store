# Copyright 2016 Shanghai Jiaotong University MSS.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


"""Storage backend for Blue Ocean storage system"""

import hashlib
import logging

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_utils import excutils
from oslo_utils import units

import glance_store
from glance_store import capabilities
from glance_store.common import utils
import glance_store.driver
from glance_store import exceptions
from glance_store.i18n import _
import glance_store.location


LOG = logging.getLogger(__name__)

DEFAULT_ADDR = 'localhost'
DEFAULT_CHUNKSIZE = 64  # in MiB

_BOSS_OPTS = [
    cfg.IntOpt('boss_store_chunk_size', default=DEFAULT_CHUNKSIZE,
               help=_('Images will be chunked into objects of this size '
                      '(in megabytes). For best performance, this should be '
                      'a power of two.')),
    cfg.StrOpt('boss_store_address', default=DEFAULT_ADDR,
               help=_('IP address of sheep daemon.'))
]


class BOSSImage(object):
    """Class describing an image stored in BOSS storage."""

    def __init__(self, addr, name, chunk_size):
        self.addr = addr
        self.name = name
        self.chunk_size = chunk_size

    def _run_command(self, command, data, *params):
        cmd = ("boss %(command)s -a d %(name)s "
               "%(params)s" %
               {"command": command,
                "name": self.name,
                "params": " ".join(map(str, params))})

        try:
            return processutils.execute(
                cmd, process_input=data, shell=True)[0]
        except processutils.ProcessExecutionError as exc:
            LOG.error(exc)
            raise glance_store.BackendException(exc)

    def get_size(self):
        """
        Return the size of the this image

        boss Usage: collie vdi list -r -a address -p port image
        """
        out = self._run_command("list -r", None)
        return int(out.split(' ')[3])

    def read(self, offset, count):
        """
        Read up to 'count' bytes from this image starting at 'offset' and
        return the data.

        boss Usage: collie vdi read -a address -p port image offset len
        """
        return self._run_command("read", None, str(offset), str(count))

    def write(self, data, offset, count):
        """
        Write up to 'count' bytes from the data to this image starting at
        'offset'

        boss Usage: collie vdi write -a address -p port image offset len
        """
        self._run_command("write", data, str(offset), str(count))

    def create(self, size):
        """
        Create this image in the boss cluster with size 'size'.

        boss Usage: collie vdi create -a address -p port image size
        """
        self._run_command("create", None, str(size))

    def resize(self, size):
        """Resize this image in the boss cluster with size 'size'.

        boss Usage: collie vdi create -a address -p port image size
        """
        self._run_command("resize", None, str(size))

    def delete(self):
        """
        Delete this image in the boss cluster

        boss Usage: collie vdi delete -a address -p port image
        """
        self._run_command("delete", None)

    def exist(self):
        """
        Check if this image exists in the boss cluster via 'list' command

        boss Usage: collie vdi list -r -a address -p port image
        """
        out = self._run_command("list -r", None)
        if not out:
            return False
        else:
            return True


class StoreLocation(glance_store.location.StoreLocation):
    """
    Class describing a BOSS URI. This is of the form:

        BOSS://addr:image # to do

    """

    def process_specs(self):
        self.image = self.specs.get('image')
        self.addr = self.specs.get('addr')

    def get_uri(self):
        return "BOSS://%(addr)s:%(image)s" % {
            'addr': self.addr,
            'image': self.image}

    def parse_uri(self, uri):
        valid_schema = 'BOSS://'
        if not uri.startswith(valid_schema):
            reason = _("URI must start with '%s'") % valid_schema
            raise exceptions.BadStoreUri(message=reason)
        pieces = uri[len(valid_schema):].split(':')
        if len(pieces) == 2:
            self.image = pieces[1]
            self.addr = pieces[0]
        # This is used for backwards compatibility.
        else:
            self.image = pieces[0]
            self.addr = self.conf.glance_store.boss_store_address


class ImageIterator(object):
    """
    Reads data from an BOSS image, one chunk at a time.
    """

    def __init__(self, image):
        self.image = image

    def __iter__(self):
        image = self.image
        total = left = image.get_size()
        while left > 0:
            length = min(image.chunk_size, left)
            data = image.read(total - left, length)
            left -= len(data)
            yield data
        raise StopIteration()


class Store(glance_store.driver.Store):
    """BOSS backend adapter."""

    # What CAP does BOSS supply
    _CAPABILITIES = (capabilities.BitMasks.RW_ACCESS |
                     capabilities.BitMasks.DRIVER_REUSABLE)
    OPTIONS = _BOSS_OPTS
    EXAMPLE_URL = "BOSS://addr:image"  ##To do

    def get_schemes(self):
        return ('BOSS',)

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`
        """

        try:
            chunk_size = self.conf.glance_store.boss_store_chunk_size
            self.chunk_size = chunk_size * units.Mi
            self.READ_CHUNKSIZE = self.chunk_size
            self.WRITE_CHUNKSIZE = self.READ_CHUNKSIZE
            self.addr = self.conf.glance_store.boss_store_address
        except cfg.ConfigFileValueError as e:
            reason = _("Error in store configuration: %s") % e
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='BOSS',
                                                   reason=reason)

        try:
            processutils.execute("BOSS", shell=True)
        except processutils.ProcessExecutionError as exc:
            reason = _("Error in store configuration: %s") % exc
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='BOSS',
                                                   reason=reason)

    @capabilities.check
    def get(self, location, offset=0, chunk_size=None, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file, and returns a generator for reading
        the image file

        :param location: `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises: `glance_store.exceptions.NotFound` if image does not exist
        """

        loc = location.store_location
        image = BOSSImage(loc.addr, loc.port, loc.image,
                              self.READ_CHUNKSIZE)
        if not image.exist():
            raise exceptions.NotFound(_("boss image %s does not exist")
                                      % image.name)
        return (ImageIterator(image), image.get_size())

    def get_size(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file and returns the image size

        :param location: `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises: `glance_store.exceptions.NotFound` if image does not exist
        :param rtype: int
        """

        loc = location.store_location
        image = BOSSImage(loc.addr, loc.image, self.READ_CHUNKSIZE)
        if not image.exist():
            raise exceptions.NotFound(_("boss image %s does not exist")
                                      % image.name)
        return image.get_size()

    @capabilities.check
    def add(self, image_id, image_file, image_size, context=None,
            verifier=None):
        """
        Stores an image file with supplied identifier to the backend
        storage system and returns a tuple containing information
        about the stored image.

        :param image_id: The opaque image identifier
        :param image_file: The image data to write, as a file-like object
        :param image_size: The size of the image data to write, in bytes
        :param verifier: An object used to verify signatures for images

        :retval: tuple of URL in backing store, bytes written, and checksum
        :raises: `glance_store.exceptions.Duplicate` if the image already
                existed
        """

        image = BOSSImage(self.addr, self.port, image_id,
                              self.WRITE_CHUNKSIZE)
        if image.exist():
            raise exceptions.Duplicate(_("boss image %s already exists")
                                       % image_id)

        location = StoreLocation({
            'image': image_id,
            'addr': self.addr,
            'port': self.port
        }, self.conf)

        image.create(image_size)

        try:
            offset = 0
            checksum = hashlib.md5()
            chunks = utils.chunkreadable(image_file, self.WRITE_CHUNKSIZE)
            for chunk in chunks:
                chunk_length = len(chunk)
                # If the image size provided is zero we need to do
                # a resize for the amount we are writing. This will
                # be slower so setting a higher chunk size may
                # speed things up a bit.
                if image_size == 0:
                    image.resize(offset + chunk_length)
                image.write(chunk, offset, chunk_length)
                offset += chunk_length
                checksum.update(chunk)
                if verifier:
                    verifier.update(chunk)
        except Exception:
            # Note(zhiyan): clean up already received data when
            # error occurs such as ImageSizeLimitExceeded exceptions.
            with excutils.save_and_reraise_exception():
                image.delete()

        return (location.get_uri(), offset, checksum.hexdigest(), {})

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file to delete

        :param location: `glance_store.location.Location` object, supplied
                  from glance_store.location.get_location_from_uri()

        :raises: NotFound if image does not exist
        """

        loc = location.store_location
        image = BOSSImage(loc.addr, loc.port, loc.image,
                          self.WRITE_CHUNKSIZE)

        if not image.exist():
            raise exceptions.NotFound(_("boss image %s does not exist") %loc.image)
        image.delete()
