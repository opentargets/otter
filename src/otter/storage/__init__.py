"""Remote storage implementation classes."""

import errno

from loguru import logger

from otter.storage.google import GoogleStorage
from otter.storage.model import RemoteStorage
from otter.storage.noop import NoopStorage


def get_remote_storage(uri: str | None) -> RemoteStorage:
    """Get a storage object for a URI.

    :param uri: The URI to get a storage object for.
    :type uri: str
    :return: A remote storage class.
    :rtype: RemoteStorage
    :raises ValueError: If the URI is not supported.
    """
    if not uri:
        return NoopStorage()

    remotes = {
        'gs': GoogleStorage,
    }

    proto = uri.split(':')[0]
    remote = remotes.get(proto, NoopStorage)()

    if type(remote) is NoopStorage and uri:
        logger.critical(f'remote storage for protocol {proto} is not supported')
        raise SystemExit(errno.ENOSYS)

    logger.debug(f'using {remote.name} as remote storage class for {uri}')
    return remote
