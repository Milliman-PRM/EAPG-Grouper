"""
### CODE OWNERS: Ben Copeland, Chas Busenburg

### OBJECTIVE:
  House methods or constants that may apply to various parts of the EAPG process

### DEVELOPER NOTES:
  <none>
"""
import logging
import typing
import os
from pathlib import Path

import pyspark.sql
import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types
from prm.spark.io_txt import build_structtype_from_csv

LOGGER = logging.getLogger(__name__)

PATH_TEMPLATES = Path(os.environ['eapg_grouper_home']) / 'templates'
PATH_INPUT_TEMPLATE = PATH_TEMPLATES / 'prm_eapgs_in.2017.1.2.dic'
PATH_OUTPUT_TEMPLATE = PATH_TEMPLATES / 'prm_eapgs_out.2017.1.2.dic'


DAYS_PER_YEAR = 365.25
N_ICD_COLUMNS = 24

try:
    _PATH_PARENT = Path(__file__).parent
except NameError: # pragma: no cover
    # Likely running interactively for development
    import eapg # pylint: disable=wrong-import-position
    _PATH_PARENT = Path(eapg.__file__).parent

PATH_SCHEMAS = _PATH_PARENT / 'schemas'

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def _get_icd_columns(
        dataframe: pyspark.sql.DataFrame,
        prefix: str,
        *,
        from_grouped_dataframe: bool=False
    ) -> typing.Iterable:
    """
        Grab ICD diag/proc/poa columns from a potentially dynamic number of columns
        with the ability to grab the first item from a grouped dataframe
    """
    LOGGER.debug('Finding %s columns from %s', prefix, dataframe)
    col_list = list()
    for i in range(1, N_ICD_COLUMNS + 1):
        potential_column = '{}{}'.format(prefix, i)
        if potential_column in dataframe.columns:
            if from_grouped_dataframe:
                _col_ref = spark_funcs.first(
                    spark_funcs.col(potential_column)
                    ).alias(potential_column)
            else:
                _col_ref = spark_funcs.col(potential_column)
            col_list.append(_col_ref)

    return col_list

def _convert_poa_to_character(
        column: pyspark.sql.Column
    ) -> pyspark.sql.Column:
    """Convert any numeric POA representations to Y/N"""
    LOGGER.debug('Converting %s to character-only POA', column)
    return spark_funcs.when(
        column.isin('Y', 'N', 'U', 'W'),
        column
        ).when(
            column == '1',
            spark_funcs.lit('Y'),
            ).otherwise('N')


def _coalesce_metadata_and_cast(
        struct: spark_types.StructType,
        dataframe: pyspark.sql.DataFrame,
    ) -> typing.Iterable:
    """
        Update the datamart schema from a dataframe schema for pass down to
        aliasWithMetadata
    """
    LOGGER.info(
        'Adding %s metadata to %s and casting to desired dataTypes',
        struct,
        dataframe,
        )
    meta_select = list()
    for field in struct:
        meta = field.metadata.copy()
        meta.update(dataframe.schema[field.name].metadata)
        meta_select.append(
            spark_funcs.col(field.name).cast(field.dataType).aliasWithMetadata(
                field.name,
                metadata=meta,
                )
            )
    return dataframe.select(meta_select)


def get_standard_inputs_from_prm(
        input_dataframes: "typing.Mapping[str, pyspark.sql.DataFrame]",
        *,
        path_schema_input: Path=PATH_SCHEMAS / 'eapgs_in.csv'
    ) -> pyspark.sql.DataFrame:
    """Get the standard EAPG inputs from PRM ~public data sets"""
    LOGGER.info("Creating standard EAPG software inputs from ~public PRM data")
    struct = build_structtype_from_csv(
        path_schema_input
        )

    diag_cols = _get_icd_columns(
        input_dataframes['outclaims_prm'],
        'icddiag',
        from_grouped_dataframe=True,
        )

    poa_cols = _get_icd_columns(
        input_dataframes['outclaims_prm'],
        'poa',
        from_grouped_dataframe=True,
        )

    LOGGER.info('Reformatting outclaims_prm into EAPG format')
    df_claim_summaries = input_dataframes['outclaims_prm'].select(
        '*',
        spark_funcs.abs(
            spark_funcs.col('paid')
            ).alias('abs_paid'), # For some reason, paid can't be negative
        spark_funcs.lit(0).alias('actionflag'),
        spark_funcs.when(
            spark_funcs.col('prm_line').startswith('P'),
            1,
            ).otherwise(0).alias('professionalserviceflag'),
        (spark_funcs.col('allowed') - spark_funcs.col('paid')).alias('noncoveredcharges'),
        spark_funcs.lit(None).alias('ndccode'),
    ).groupBy(
        'member_id',
        'claimid',
    ).agg(
        *[
            spark_funcs.concat_ws(
                ';',
                spark_funcs.collect_list(
                    spark_funcs.coalesce(
                        spark_funcs.col(col),
                        spark_funcs.lit(''), # Empty string to force semi-colon delimiter
                        )
                    )
                ).alias('{}_concat'.format(col))
            for col in {
                'sequencenumber',
                'hcpcs',
                'modifier',
                'modifier2',
                'revcode',
                'units',
                'abs_paid',
                'fromdate',
                'actionflag',
                'professionalserviceflag',
                'noncoveredcharges',
                'ndccode',
                'patientpay',
                'pos',
                }
            ],
        spark_funcs.first('providerid').alias('nationalprovideridentifier'),
        spark_funcs.first('prm_fromdate_claim').alias('admitdate'),
        spark_funcs.first('prm_todate_claim').alias('dischargedate'),
        spark_funcs.first('billtype').alias('typeofbill'),
        spark_funcs.first('dischargestatus').alias('dischargestatus'),
        spark_funcs.sum('mr_paid').alias('totalcharges'),
        *diag_cols,
        spark_funcs.substring(
            spark_funcs.first('icdversion'),
            2,
            1,
            ).alias('icdversionqualifier'),
        *poa_cols,
        spark_funcs.first('providerzip').alias('providerzipcode'),
        spark_funcs.max('prm_prv_id_operating').alias('operatingphysician'),
        spark_funcs.first('prm_line').alias('prm_line'),
    )

    grouped_diag_cols = _get_icd_columns(
        df_claim_summaries,
        'icddiag',
        )

    grouped_poa_cols = _get_icd_columns(
        df_claim_summaries,
        'poa',
        )

    df_decor = df_claim_summaries.select(
        spark_funcs.lit(None).alias('patientname'),
        spark_funcs.lit(None).alias('accountnumber'),
        spark_funcs.col('sequencenumber_concat').alias('medicalrecordnumber'),
        spark_funcs.col('nationalprovideridentifier'),
        spark_funcs.col('admitdate'),
        spark_funcs.col('dischargedate'),
        # birthdate joined from member
        # ageinyears must be derived from dob/birthdate
        # sex joined from member
        spark_funcs.col('typeofbill'),
        spark_funcs.lit(None).alias('conditioncode'),
        spark_funcs.col('dischargestatus'),
        spark_funcs.lit(1).alias('userkey1'), # Just placeholders
        spark_funcs.lit(2).alias('userkey2'),
        spark_funcs.lit(3).alias('userkey3'),
        spark_funcs.col('totalcharges'),
        grouped_diag_cols[0].alias('principaldiagnosis'),
        spark_funcs.concat_ws(
            ';',
            *grouped_diag_cols[1:],
            ).alias('secondarydiagnosis'),
        spark_funcs.lit(None).alias('reasonforvisitdiagnosis'),
        spark_funcs.col('hcpcs_concat').alias('procedurehcpcs'),
        spark_funcs.col('modifier_concat').alias('itemmodifier1'),
        spark_funcs.col('modifier2_concat').alias('itemmodifier2'),
        spark_funcs.lit(None).alias('itemmodifier3'),
        spark_funcs.lit(None).alias('itemmodifier4'),
        spark_funcs.lit(None).alias('itemmodifier5'),
        spark_funcs.col('revcode_concat').alias('itemrevenuecode'),
        spark_funcs.col('units_concat').alias('itemunitsofservice'),
        spark_funcs.col('abs_paid_concat').alias('itemcharges'),
        spark_funcs.col('fromdate_concat').alias('itemservicedate'),
        spark_funcs.col('actionflag_concat').alias('itemactionflag'),
        spark_funcs.lit(None).alias('occurrencecode'),
        spark_funcs.lit(None).alias('occurrencecodedate'),
        spark_funcs.col('professionalserviceflag_concat').alias('itemprofessionalserviceflag'),
        spark_funcs.col('icdversionqualifier'),
        _convert_poa_to_character(
            grouped_poa_cols[0]
            ).alias('principaldiagnosispoa'),
        spark_funcs.concat_ws(
            ';',
            *[
                _convert_poa_to_character(
                    poa_col
                    )
                for poa_col in grouped_poa_cols[1:]
                ],
            ).alias('secondarydiagnosispoa'),
        spark_funcs.lit(None).alias('valuecode'),
        spark_funcs.lit(None).alias('valuecodeamount'),
        spark_funcs.lit(9).alias('admitpriority'),
        spark_funcs.col('noncoveredcharges_concat').alias('itemnoncoveredcharges'),
        spark_funcs.lit(None).alias('filler_formerlypointoforigin'),
        spark_funcs.col('providerzipcode'),
        spark_funcs.lit(None).alias('itemndccode'),
        spark_funcs.lit(None).alias('userdefinednumberofvisits'), # Maryland only
        spark_funcs.col('operatingphysician'),
        spark_funcs.col('member_id').alias('patientid'),
        spark_funcs.lit(None).alias('externalcauseofinjurydiagnosis'),
        spark_funcs.lit(None).alias('externalcauseofinjurydiagnosispoa'),
        spark_funcs.lit(None).alias('drugcharges'), # Maryland only
        spark_funcs.lit(None).alias('observationcharges'), # Maryland only
        spark_funcs.lit(None).alias('observationunits'), # Maryland only
        spark_funcs.lit(None).alias('supplycharges'), # Maryland only
        spark_funcs.lit(None).alias('claimtype'), # Depends on state, MI or WI
        spark_funcs.lit(None).alias('financialpayer'), # Depends on state, NY or WI
        spark_funcs.lit(None).alias('memberbenefitplan'), # WI only
        spark_funcs.col('patientpay_concat').alias('itemcostshare'), # WI only
        spark_funcs.col('pos_concat').alias('itemplaceofservice'), # WI only
    )

    LOGGER.info('Joining member dimensions onto reformatted claims')
    df_merge = df_decor.join(
        input_dataframes['member'].select(
            spark_funcs.col('member_id').alias('patientid'),
            spark_funcs.col('gender').alias('sex'),
            spark_funcs.col('dob').alias('birthdate'),
            ),
        'patientid',
        how='left_outer'
    ).withColumn(
        'ageinyears',
        spark_funcs.floor(
            spark_funcs.datediff(
                spark_funcs.col('admitdate'),
                spark_funcs.col('birthdate')
                ) / DAYS_PER_YEAR
            ).alias('ageinyears'),
    )

    df_eapg_input = _coalesce_metadata_and_cast(
        struct,
        df_merge
        )

    return df_eapg_input

if __name__ == 'main':
    pass
