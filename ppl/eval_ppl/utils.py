import numpy as np
import pandas as pd

INDEX_TYPES = {
    "float": float,
    "double": np.double,
    "boolean": bool,
    "keyword": str,
    "long": pd.Int64Dtype(),
    "bigint": pd.Int64Dtype(),
    "text": str,
    "object": str,
    "struct": str,
    "integer": pd.Int32Dtype(),
    "timestamp": str,
    "time": str,
    "datetime": str,
    "date": str,
    "string": str,
    "array": str,
    "ip": str,
    "geo_point": str,
    "demo": str,
    "int": int,
}


# def simple_parse(datarows, schema):
#     names = [x["name"] for x in schema]
#     typing = {x["name"]: INDEX_TYPES[x["type"]] for x in schema}
#     return pd.DataFrame(datarows, columns=names).astype(typing)
def simple_parse(datarows, schema):
    names = [x["name"] for x in schema]
    # First create the DataFrame without type conversion
    df = pd.DataFrame(datarows, columns=names)

    # Then convert each column individually with proper null handling
    for col in schema:
        col_name = col["name"]
        col_type = col["type"]
        if col_type == "int":
            # Use pandas' nullable integer type
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
        else:
            # For other types, use the mapping if the column doesn't contain nulls
            if col_name in INDEX_TYPES and not df[col_name].isna().any():
                df[col_name] = df[col_name].astype(INDEX_TYPES[col_type])

    return df


def eval_execution_accuracy(pred_df, gold_df):
    """
    Problem: Data from two queries can be "same" but with:
        - permuted rows
        - permuted columns
        - identical columns but different column names

    Approach: Cast to pd.DataFrame and check for equivalence
    up to row and column permutation. Also checks types column-wise.
    """

    target_dtypes = pred_df.dtypes

    # Step 1: compare columns
    # best case: columns are the same and we can skip this part
    if set(gold_df.columns) != set(pred_df.columns):
        # otherwise try to find 1-1 mapping between df columns

        # start by subtracting out any columns with the same name in each df
        common_cols = list(set(gold_df.columns).intersection(set(pred_df.columns)))
        gold_cols = [x for x in list(gold_df.columns) if x not in common_cols]
        possible_col_matches = [
            x for x in list(pred_df.columns) if x not in common_cols
        ]

        # then look for 1-1 mapping among remaining cols
        for col in gold_cols:
            unmatched = True
            for pcol in possible_col_matches:
                try:  # first try to cast gold col to pred col type
                    gold_df = gold_df.astype({col: pred_df.dtypes[pcol]})

                    # round float columns to same number of decimals
                    if target_dtypes[pcol] == float:
                        gold_df = gold_df.round({col: 4})
                        pred_df = pred_df.round({pcol: 4})

                except:  # if it doesn't work, continue to next possible match
                    continue

                if set(gold_df[col].values) == set(
                    pred_df[pcol].values
                ):  # slightly weak
                    possible_col_matches.remove(pcol)
                    pred_df = pred_df.rename(
                        columns={pcol: col}
                    )  # rename pred col to match gold col

                    unmatched = False
                    break

            if unmatched:  # gold col not in pred cols: dfs can't be equal
                return False
        if len(possible_col_matches) > 0:
            return False  # pred df has extra, unmatched cols

    else:
        try:  # try to cast each gold col to its corresponding pred col type
            gold_df = gold_df.astype(
                {col: dtype for (col, dtype) in zip(pred_df.columns, pred_df.dtypes)}
            )

            # round float columns to same number of decimals
            for col in pred_df.columns:
                if pred_df.dtypes[col] == float:
                    gold_df = gold_df.round({col: 4})
                    pred_df = pred_df.round({col: 4})
        except:
            return False

    # Step 2: compare rows via outer join
    try:
        merged = gold_df.merge(
            pred_df, on=list(gold_df.columns), how="outer", indicator="exist"
        )
        # "exist" col in merged says whether the row came from left df, right df, or both
        return np.all(merged.exist == "both")

    except Exception:
        return False


def match_columns(df_A, df_B):
    """
    Does every column in df_A correspond to a column in df_B?
    """
    dtypes = df_A.dtypes
    common_cols = list(set(df_A.columns).intersection(set(df_B.columns)))

    # for each column with a common name: do their values match between dfs?
    for col in common_cols:
        try:
            # cast cols to same datatype
            df_B = df_B.astype({col: df_A.dtypes[col]})
            if dtypes[col] == float:  # round float cols to same number of decimals
                df_A = df_A.round({col: 4})
                df_B = df_B.round({col: 4})
        except:
            return "Column type mismatch"

        if set(df_B[col].values) == set(df_A[col].values):
            continue
        else:
            return "Column value mismatch"

    # for remaining columns: can each one be matched to one of the remaining gold cols?
    remaining_gold_cols = [x for x in list(df_B.columns) if x not in common_cols]
    remaining_cols = [x for x in list(df_A.columns) if x not in common_cols]

    for col in remaining_cols:
        unmatched = True
        for pcol in remaining_gold_cols:
            try:  # try to cast pcol to col type
                df_B = df_B.astype({pcol: df_A.dtypes[col]})
                if dtypes[col] == float:  # round float cols to same number of decimals
                    df_A = df_A.round({col: 4})
                    df_B = df_B.round({col: 4})
            except:
                continue

            if set(df_B[pcol].values) == set(df_A[col].values):
                unmatched = False
                remaining_gold_cols.remove(pcol)
                break

        if unmatched:
            return "Unmatched col in A"

    # all match
    return "B contains A"


def match_rows(df_A, df_B):
    """
    Does every row in df_A correspond to a row in df_B?
    """
    dtypes = df_A.dtypes
    common_cols = list(set(df_A.columns).intersection(set(df_B.columns)))

    for col in common_cols:
        try:
            df_B = df_B.astype({col: df_A.dtypes[col]})
            if dtypes[col] == float:  # round float cols to same number of decimals
                df_A = df_A.round({col: 4})
                df_B = df_B.round({col: 4})
        except:
            return "Column type mismatch"

    # first try to match columns, including casting to common type
    if not len(common_cols) == len(df_A.columns):
        remaining_cols_a = [x for x in list(df_A.columns) if x not in common_cols]
        remaining_cols_b = [x for x in list(df_B.columns) if x not in common_cols]

        for col in remaining_cols_a:
            unmatched = True
            for pcol in remaining_cols_b:
                try:  # try to cast pcol to col type
                    df_B = df_B.astype({pcol: df_A.dtypes[col]})
                    if (
                        dtypes[col] == float
                    ):  # round float cols to same number of decimals
                        df_A = df_A.round({col: 4})
                        df_B = df_B.round({col: 4})
                except:
                    continue

                if set(df_B[pcol].values) == set(df_A[col].values):
                    unmatched = False
                    remaining_cols_b.remove(pcol)
                    df_B = df_B.rename(columns={pcol: col})
                    break

            if unmatched:
                return "Column value mismatch"

    # assert: each row in A matches AT LEAST ONE row in B
    for row in df_A.itertuples(index=False):
        tst = df_B == row
        if sum(tst.all(axis=1)) == 0:  # no matches
            return "Other"

    return "B contains A"


def categorize_exec_inacc(pred_df, gold_df):

    rpred, cpred = pred_df.shape
    rgold, cgold = gold_df.shape

    if rpred == rgold and cpred == cgold:
        shape_status = "Same shape"
        details = ""

        # check for NAs
        pred_na = pred_df.isna().any().any()
        pred_none = (pred_df == "None").any().any()
        gold_na = gold_df.isna().any().any()
        if pred_na or gold_na:
            details = "NAs in result"
        elif pred_none:
            details = "None in pred"
        else:
            details = "Other"

    elif rpred == rgold:
        shape_status = "Same row count"

        if cpred > cgold:
            match_res = match_columns(gold_df, pred_df)
            if match_res == "B contains A":
                details = "Pred contains all gold cols"
            else:
                details = "Other"

        elif cgold > cpred:
            match_res = match_columns(pred_df, gold_df)
            if match_res == "B contains A":
                details = "Gold contains all pred cols"
            else:
                details = "Other"

        else:
            details = None

    elif cpred == cgold:
        shape_status = "Same col count"

        if rpred > rgold:
            match_res = match_rows(gold_df, pred_df)
            if match_res == "B contains A":
                details = "Pred contains all gold rows"
            else:
                details = "Other"

        elif rgold > rpred:
            match_res = match_rows(pred_df, gold_df)
            if match_res == "B contains A":
                details = "Gold contains all pred rows"
            else:
                details = "Other"

    else:
        shape_status = "Row and col mismatch"
        details = None

    return shape_status, details
