from pandas import DataFrame


def print_table(df: DataFrame, top_n: int = 10, max_col_width = 1000, print_callback = print):
    from tabulate import tabulate
    from copy import deepcopy
    print_df = deepcopy(df.head(top_n))
    list_type_int64 = (
        print_df.dtypes
        .reset_index(name="COLUMN_TYPE")
        .assign(COLUMN_TYPE=lambda x: x.COLUMN_TYPE.apply(lambda y: str(y)))
        .query("COLUMN_TYPE.eq('Int64')")["index"]
        .values.tolist()
    )
    print_df[list_type_int64] = print_df[list_type_int64].astype("str")
    print_df.fillna("NULL")

    for col in print_df.select_dtypes(include=['object', 'string', 'category']).columns:
        print_df[col] = print_df[col].apply(lambda x: x[:max_col_width] + "..." if isinstance(x, str) and len(x) > max_col_width else x)

    print_callback("\n" + tabulate(print_df, headers='keys', tablefmt='psql', floatfmt=',.6f'))


def format_table(df: DataFrame, top_n: int = 10) -> str:
    from tabulate import tabulate
    return tabulate(df[:top_n], headers='keys', tablefmt='psql', floatfmt=',.6f')



