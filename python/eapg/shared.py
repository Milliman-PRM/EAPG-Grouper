"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
  House methods or constants that may apply to various parts of the EAPG process

### DEVELOPER NOTES:
  <none>
"""
import logging
import typing
from pathlib import Path

import pyspark.sql
import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types
from prm.spark.io_txt import build_structtype_from_csv

LOGGER = logging.getLogger(__name__)

DAYS_PER_YEAR = 365.25
N_DIAG_COLUMNS = 12

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


def _coalesce_metadata_and_cast(
        struct: spark_types.StructType,
        dataframe: pyspark.sql.DataFrame,
    ) -> typing.Iterable:
    """
        Update the datamart schema from a dataframe schema for pass down to
        aliasWithMetadata
    """
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
        (spark_funcs.col('allowed') - spark_funcs.col('billed')).alias('noncoveredcharges'),
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
        *[
            spark_funcs.first('icddiag{}'.format(i)).alias('icddiag{}'.format(i))
            for i in range(1, N_DIAG_COLUMNS + 1)
            ],
        spark_funcs.substring(
            spark_funcs.first('icdversion'),
            2,
            1,
            ).alias('icdversionqualifier'),
        *[
            spark_funcs.when(
                spark_funcs.first('poa{}'.format(i)).isin('Y', 'W'),
                spark_funcs.lit('Y')
                ).otherwise('N').alias('poa{}'.format(i))
            for i in range(1, 16)
            ],
        spark_funcs.first('providerzip').alias('providerzipcode'),
        spark_funcs.first('prm_prv_id_operating').alias('operatingphysician'),
        spark_funcs.first('prm_line').alias('prm_line'),
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
        spark_funcs.lit(1).alias('userkey1'),
        spark_funcs.lit(2).alias('userkey2'),
        spark_funcs.lit(3).alias('userkey3'),
        spark_funcs.col('totalcharges'),
        spark_funcs.col('icddiag1').alias('principaldiagnosis'),
        spark_funcs.concat_ws(
            ';',
            *[
                spark_funcs.col('icddiag{}'.format(i))
                for i in range(2, N_DIAG_COLUMNS + 1)
                ],
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
        spark_funcs.when(
            spark_funcs.col('poa1').isin('Y', 'N', 'U', 'W'),
            spark_funcs.col('poa1')
            ).when(
                spark_funcs.col('poa1') == '1',
                spark_funcs.lit('Y'),
                ).otherwise('N').alias('principaldiagnosispoa'),
        spark_funcs.concat_ws(
            ';',
            *[
                spark_funcs.when(
                    spark_funcs.col('poa{}'.format(i)).isin('Y', 'N', 'U', 'W'),
                    spark_funcs.col('poa{}'.format(i))
                    ).when(
                        spark_funcs.col('poa{}'.format(i)) == '1',
                        spark_funcs.lit('Y'),
                        ).otherwise('N')
                for i in range(2, N_DIAG_COLUMNS + 1)
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

    df_struct = _coalesce_metadata_and_cast(
        struct,
        df_merge
        )

    return df_struct

if __name__ == 'main':
    pass
