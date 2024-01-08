# flake8: noqa
# Use this class to transfer files using sftp.
# Multiple attempts are made for sftp operations if needed.
# A 10 second delay is used to start with between tries.
#
# TME  08/19/19  Initial version
# TME  08/29/19  Added get_file()
# TME  10/06/20  Now extending pysftp.Connection(). Removed the connect
#                and disconnect routines. Added put_dir().

from pysftp import Connection
from retrying import retry


class xfer_files(Connection):  # pragma: no cover
    def __init__(self, remoteSite, remoteUser, privateKey=False,
                 password=False, sshPort=22):
        self.remoteSite = remoteSite
        self.remoteUser = remoteUser
        self.privateKey = privateKey
        self.password = password
        self.sshPort = sshPort
        self._sftp_live = False
        self._transport = None
        self.error = False

        # Connect to remote site
        if self.privateKey:
            try:
                super().__init__(remoteSite, username=remoteUser,
                                 private_key=privateKey, port=sshPort)
                self.error = False
            except Exception as e:
                self.error = 'Key login failed: %s@%s : %s' % \
                    (self.remoteUser, self.remoteSite, e)
                print("CANT LOGIN ----")
                print(e)
                return None
        elif self.password:
            try:
                super().__init__(remoteSite, username=remoteUser,
                                 password=password, port=sshPort)
                self.error = False
            except Exception:
                self.error = 'PW login failed: %s@%s' % \
                    (self.remoteUser, self.remoteSite)
                return None
        else:
            self.error = 'A private key or password must be specified'
            print(self.error)
            return None

    # Sftp get file from remote system.
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=10000,
           wait_exponential_max=100000)
    def get_file(self, remoteFile, localFile):
        try:
            self.get(remoteFile, localFile)
            self.error = False
        except Exception:
            self.error = 'Failed to download %s@%s:%s to %s' % \
                (self.remoteUser, self.remoteSite, remoteFile, localFile)
            return None

    # Sftp put file to remote system.
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=10000,
           wait_exponential_max=100000)
    def put_file(self, localFile, remoteFile):
        try:
            self.put(localFile, remoteFile)
            self.error = False
        except Exception:
            self.error = 'Failed to send %s to %s@%s:%s' % \
                (localFile, self.remoteUser, self.remoteSite, remoteFile)
            return None

    # Sftp put a directory recursively to a remote system.
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=10000,
           wait_exponential_max=100000)
    def put_dir(self, localDir, remoteDir, permissions=False):

        # Make directory. Set permissions if requested.
        try:
            self.makedirs(remoteDir)
            self.error = False
        except Exception:
            self.error = 'Failed to make %s@%s:%s' % \
                (self.remoteUser, self.remoteSite, remoteDir)
            return None

        if permissions:
            try:
                self.chmod(remoteDir, permissions)
                self.error = False
            except Exception:
                self.error = 'Failed to set permissions of %s on %s@%s:%s' % \
                    (permissions, self.remoteUser, self.remoteSite, remoteDir)
                return None

        # And then copy directory recursively
        try:
            self.put_r(localDir, remoteDir)
            self.error = False
        except Exception:
            self.error = 'Failed to copy %s to %s@%s:%s' % \
                (localDir, self.remoteUser, self.remoteSite, remoteDir)
            return None
