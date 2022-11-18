#
# Copyright (C) 2022  Hiroaki Shiino
#
# Full copyright notice can be found in LICENSE.
#

import dask.dataframe as dd
import pandas as pd

from PyClick.pyclick.click_models.task_centric.TaskCentricSearchSession import TaskCentricSearchSession
from PyClick.pyclick.search_session.SearchResult import SearchResult

__author__ = 'Hiroaki Shiino'


class AbemaTopModulesParser:
    """
    """

    @staticmethod
    def parse(sessions_csv_path,
              req_id_col_name,
              sessions_max=None,
              query_col_name=None,
              max_pos=70,
              ):

        """
        Parses recommendation sessions, formatted according to the make_abema_billboard_query_session.ipynb).
        Returns a list of SearchSession objects.

        """

        df = dd.read_csv(sessions_csv_path)

        if query_col_name is None:
            series_session = df.apply(lambda x: set_session(x, x[req_id_col_name], max_pos), axis=1, meta=(None, 'object'))
        else:
            series_session = df.apply(lambda x: set_session(x, x[req_id_col_name], max_pos,query_col_name), axis=1, meta=(None, 'object'))

        return series_session


    @staticmethod
    def parse_form_dataframe(df,
              req_id_col_name,
              sessions_max=None,
              query_col_name=None,
              max_pos=70,
              ):
        """
        Parses recommendation sessions, formatted according to the make_abema_billboard_query_session.ipynb).
        Returns a list of SearchSession objects.

        """
        if query_col_name is None:
            series_session = df.apply((lambda x: set_session(x, x[req_id_col_name], max_pos)), axis=1, meta=(None, 'object'))
        else:
            series_session = df.apply((lambda x: set_session(x, x[req_id_col_name], max_pos,query_col_name)), axis=1, meta=(None, 'object'))

        return series_session

def set_session(df, req_name, max_pos, query_col_name=None):
    if query_col_name is None:
        session = TaskCentricSearchSession(req_name, "0")
    else:
        session = TaskCentricSearchSession(req_name, df[query_col_name])
    for pos in range(max_pos):
        item = df["pos{}".format(pos)]
        label = df["label{}".format(pos)]
        if label != -1 or item != -1:
            session.web_results.append(SearchResult(item, label))
    return session
