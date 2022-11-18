#
# Copyright (C) 2022  Hiroaki Shiino
#
# Full copyright notice can be found in LICENSE.
#
import pandas as pd

from PyClick.pyclick.click_models.task_centric.TaskCentricSearchSession import TaskCentricSearchSession
from PyClick.pyclick.search_session.SearchResult import SearchResult

__author__ = 'Hiroaki Shiino'


class AbemaBillBoardParser:
    """
    """

    @staticmethod
    def parse(sessions_pkl_filename,
              sessions_max=None,
              query_id_col_name='query_id',
              req_id_col_name='top_req_id',
              item_id_col_name='linking_id',
              pos_col_name='pos',
              click_col_name='is_click',
              f_add_dummy=True,
              session_items_max=20,
              dummy_item=b'dummy'):

        """
        Parses recommendation sessions, formatted according to the make_abema_billboard_query_session.ipynb).
        Returns a list of SearchSession objects.

        """

        df = pd.read_pickle(sessions_pkl_filename)[[query_id_col_name,
                                                    req_id_col_name,
                                                    item_id_col_name,
                                                    pos_col_name,
                                                    click_col_name]]
        sessions = []
        n_non_continuous_sessions = 0

        for key, rows in df.groupby([query_id_col_name,req_id_col_name]):
            if sessions_max and len(sessions) >= sessions_max:
                break

            session = TaskCentricSearchSession(key[1], key[0])
            n_results = rows[pos_col_name].max()+1
            # session.web_results = [0] * (rows[pos_col_name].max()+1)

            for i, row in rows.sort_values(pos_col_name).reset_index().iterrows():
                # if i != row[pos_col_name]:
                #     n_non_continuous_sessions += 1
                #     # print('{}-session does not contains continuous results'.format(key))
                #     continue

                result = row[item_id_col_name]

                if row[click_col_name]:
                    session.web_results.append(SearchResult(result, 1))
                else:
                    session.web_results.append(SearchResult(result, 0))
            if f_add_dummy:
                while i < session_items_max-1:
                    session.web_results.append(SearchResult(dummy_item, 0))
                    i += 1
                if len(session.web_results) != session_items_max:
                    n_non_continuous_sessions += 1
                    # print('{}-session does not contains continuous results'.format(key))
                    continue
            else:
                if len(session.web_results) != n_results:
                    n_non_continuous_sessions += 1
                    # print('{}-session does not contains continuous results'.format(key))
                    continue

            sessions.append(session)

        print('{} sessions do not contain continuous results'.format(n_non_continuous_sessions))

        return sessions
