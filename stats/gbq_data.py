from .utils import GBQ
import logging
import re
import os


class GBQData:
    def __init__(self,
                 gbq_path: str,
                 sa_path: str,
                 logger: logging.Logger):

        self.logger = logger

        # needed to check credentials first, gbq_path will check if table exist and needs connection ready
        self.sa_path = sa_path
        self.gbq_path = gbq_path

        # setup inside gbq_path setter if path is correct
        self.project_id = None
        self.dataset_id = None
        self.table_id = None
        self.gbq = None

    @property
    def sa_path(self):
        return self._sa_path

    @sa_path.setter
    def sa_path(self, sa_path):
        """
        Validating if sa_path leads to correct service account
        """
        if sa_path is None:
            raise WrongServiceAccountPathException("Path to service account is empty")

        if not os.path.isfile(sa_path):
            raise WrongServiceAccountPathException(f"Did not find {sa_path}")

        if not sa_path.endswith('.json'):
            raise WrongServiceAccountPathException(f"File path {sa_path} doesnt end up with .json")

        self._sa_path = sa_path

    @property
    def gbq_path(self):
        return self._gbq_path

    @gbq_path.setter
    def gbq_path(self, gbq_path):
        """
        Validating if path is project.dataset.table format and if table exists
        """
        if gbq_path is None:
            raise GBQWrongPathPatternException("GBQ path is empty")

        if not re.search(r"([A-Z,a-z,0-9,\-,\_]+)\.([A-Z,a-z,0-9,\-,\_]+)\.([A-Z,a-z,0-9,\-,\_]+)", gbq_path):
            raise GBQWrongPathPatternException(f"GBQ path {gbq_path} doesnt seem to have project.dataset.table pattern")

        gbq_path_list = gbq_path.split('.')
        self.project_id = gbq_path_list[0]
        self.dataset_id = gbq_path_list[1]
        self.table_id = gbq_path_list[2]

        # setting up connection
        self._setup_gbq()
        if not self.gbq.check_if_table_exists(dataset_id=self.dataset_id, table_id=self.table_id):
            raise GBQTableNotExistException(f'GBQ table {gbq_path} doesnt exist')

        self._gbq_path = gbq_path

    def _setup_gbq(self) -> None:
        """
        Sets up GBQ client/utils if provided data source is GBQ table
        """
        self.gbq = GBQ(project_id=self.project_id,
                       sa_credentials=self.sa_path,
                       logger=self.logger)




class GBQWrongPathPatternException(Exception):
    pass


class GBQTableNotExistException(Exception):
    pass


class WrongServiceAccountPathException(Exception):
    pass
