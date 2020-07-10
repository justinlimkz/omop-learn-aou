import sys
sys.path.append('..')

import time
import sparse
import pandas as pd
import numpy as np
from scipy.sparse import coo_matrix

import config 

class Feature():

    def __init__(
        self,
        feature_sql_file,
        feature_sql_params,
        temporal=True
    ):
        self.is_temporal = temporal
        
        self.params = feature_sql_params
        with open(feature_sql_file, 'r') as f:
            raw_sql = f.read()
            
        self._feature_sql_file = feature_sql_file
        self._sql_raw = raw_sql


    def __str__(self):
        return "Temporal feature extracted from {}".format(
            self._feature_sql_file
        )
    
    
class FeatureSet():
    
    def __init__(
        self,
        db,
        dtcols=(
            'feature_start_date',
            'person_start_date',
            'person_end_date'
            ),
        id_col = 'person_id',
        time_col = 'feature_start_date',
        feature_col = 'concept_name'
        ):
        
        self._db = db
        self._dtcols = dtcols
        
        self.id_col = id_col
        self.time_col = time_col
        self.feature_col = feature_col
        
        self._temporal_features = []
        self._nontemporal_features = []
        
        self._temporal_feature_names = []
        self._temporal_feature_names_set = set()
        
        self._nontemporal_feature_names = []
        self._nontemporal_feature_names_set = set()
        
        self._spm_arr = []
        self.id_map = None
        self.id_map_rev = None
        self.concept_map = None
        self.concept_map_rev = None
        self.time_map = None
        self.time_map_rev = None

    def add(self, feature):
        if feature.is_temporal:
            self._temporal_features.append(feature)
        else:
            self._nontemporal_features.append(feature)

    def add_default_features(self, default_features, omop_cdm_schema=None, cohort_table_name=None):
        fns = [
            './omop-learn/sql/Features/{}.sql'.format(f)
            for f in default_features
        ]
        for fn in fns:
            feature = Feature(
                fn,
                {
                    'cdm_schema': omop_cdm_schema,
                    'cohort_table': cohort_table_name
                }
            )
            self.add(feature)
            
    def get_feature_names(self):
        return self._temporal_feature_names +  self._nontemporal_feature_names

    def get_num_features(self):
        return len (
            self._temporal_feature_names +  self._nontemporal_feature_names
        )

    def build(self, 
              omop_cdm_schema=None, 
              cohort_table_name=None,
              cohort_generation_script=None,
              cohort_generation_kwargs=None,
              first=None,
              verbose=True,
              outcome_col_name='y',
              cache_file='/tmp/store.csv', 
              from_cached=False):
        
        # Generate cohort here
        with open(cohort_generation_script, 'r') as f:
            cohort_generation_sql_raw = f.read()
        
        sep_col = self.id_col
        joined_sql = "with {} as ({}) {} order by {} asc".format(cohort_table_name, 
                                                                  cohort_generation_sql_raw.format(**cohort_generation_kwargs),
            " union all ".join(
                    f._sql_raw.format(
                        cdm_schema=omop_cdm_schema,
                        cohort_table=cohort_table_name
                    )
                for f in self._temporal_features
            ),
            ",".join([sep_col, self.time_col, self.feature_col])
        )
        if not from_cached:
#             copy_sql = """
#                 copy 
#                     ({query})
#                 to 
#                     stdout 
#                 with 
#                     csv {head}
#             """.format(
#                 query=joined_sql,
#                 head="HEADER"
#             )
#             t = time.time()
            conn = self._db.engine.raw_connection()
#             cur = conn.cursor()
#             store = open(cache_file,'wb')
#             cur.copy_expert(copy_sql, store)
            result = pd.read_sql(joined_sql, conn)
            result.to_csv(cache_file)
#             store.seek(0)
            print('Data loaded to buffer in {0:.2f} seconds'.format(
                time.time()-t
            ))
            
        t = time.time()
        store = open(cache_file,'rb')
    
        self.concepts = set()
        self.times = set()
        self.seen_ids=set()
        chunksize = int(2e6) 
        for chunk in pd.read_csv(store, chunksize=chunksize):
            self.concepts = self.concepts.union(set(chunk[self.feature_col].unique()))
            self.times = self.times.union(set(chunk[self.time_col].unique()))
            self.seen_ids = self.seen_ids.union(set(chunk[self.id_col].unique()))
        self.times = sorted(list(self.times))
        self.concepts = sorted(list(self.concepts))
        self.seen_ids = sorted(list(self.seen_ids))
        print('Got Unique Concepts and Timestamps in {0:.2f} seconds'.format(
            time.time()-t
        ))
        
        t = time.time()
        store.seek(0)
        # self.ids = cohort._cohort[self.id_col].unique()
        self.id_map = {i:person_id for i,person_id in enumerate(self.seen_ids)}
        self.id_map_rev = {person_id:i for i,person_id in enumerate(self.seen_ids)}
        self.concept_map = {i:concept_name for i,concept_name in enumerate(self.concepts)}
        self.concept_map_rev = {concept_name:i for i,concept_name in enumerate(self.concepts)}
        self.time_map = {i:t for i,t in enumerate(self.times)}
        self.time_map_rev = {t:i for i,t in enumerate(self.times)}

        print('Created Index Mappings in {0:.2f} seconds'.format(
            time.time()-t
        ))

        t = time.time()
        last = None
        spm_stored = None
        spm_arr = []
        self.recorded_ids = set()
        for chunk_num, chunk in enumerate(pd.read_csv(store, chunksize=chunksize)):
            first = chunk.iloc[0][sep_col]

            vals = chunk[sep_col].unique()
            indices = np.searchsorted(chunk[sep_col], vals)
            self.recorded_ids = self.recorded_ids.union(set(vals))
            
            chunk.loc[:, self.feature_col] = chunk[self.feature_col].apply(self.concept_map_rev.get)
            chunk.loc[:, self.time_col] = chunk[self.time_col].apply(self.time_map_rev.get)

            df_split = [
                chunk.iloc[indices[i]:indices[i+1]]
                for i in range(len(indices) - 1)
            ] +  [chunk.iloc[indices[-1]:]]

            def gen_sparr(sub_df):
                sparr = coo_matrix(
                    (
                        np.ones(len(sub_df[self.feature_col])),
                        (sub_df[self.feature_col], sub_df[self.time_col])
                    ),
                    shape=(len(self.concepts), len(self.times))
                )
                return sparr
            spm_local = [gen_sparr(s) for s in df_split]
            if first == last:
                spm_local[0] += spm_stored
            else:
                if spm_stored is not None:
                    spm_arr.append(spm_stored)
            spm_arr += spm_local[:-1]
            spm_stored = spm_local[-1]
            last = chunk.iloc[-1][sep_col]
        spm_arr.append(spm_stored)
        print(len(spm_arr))
        self._spm_arr = sparse.stack([sparse.COO.from_scipy_sparse(m) for m in spm_arr], 2)
        print('Generated Sparse Representation of Data in {0:.2f} seconds'.format(
            time.time() - t
        ))

    def get_sparr_rep(self):
        return self._spm_arr
        


def postprocess_feature_matrix(cohort, featureSet):
    feature_matrix_3d = featureSet.get_sparr_rep()
    outcomes = cohort._cohort.set_index('person_id').loc[
        sorted(featureSet.seen_ids)
    ]['y']
    good_feature_ix = [
        i for i in sorted(featureSet.concept_map)
        if '- No matching concept' not in featureSet.concept_map[i]
    ]
    good_feature_names = [
        featureSet.concept_map[i] for i in sorted(featureSet.concept_map)
        if '- No matching concept' not in featureSet.concept_map[i]
    ]
    good_time_ixs = [
        i for i in sorted(featureSet.time_map)
        if featureSet.time_map[i] <= cohort._cohort_generation_kwargs['training_end_date']
    ]
    feature_matrix_3d = feature_matrix_3d[good_feature_ix, :, :]
    feature_matrix_3d = feature_matrix_3d[:, good_time_ixs, :]
    feature_matrix_3d_transpose = feature_matrix_3d.transpose((2,1,0))
    total_events_per_person = feature_matrix_3d_transpose.sum(axis=-1).sum(axis=-1)
    people_with_data_ix = np.where(total_events_per_person.todense() > 0)[0].tolist()
    feature_matrix_3d_transpose = feature_matrix_3d_transpose[people_with_data_ix, :, :]
    outcomes_filt = outcomes.loc[[featureSet.id_map[i] for i in people_with_data_ix]]
    remap = {
        'id':people_with_data_ix,
        'time':good_time_ixs,
        'concept':good_feature_ix
    }
    return outcomes_filt, feature_matrix_3d_transpose, remap, good_feature_names