import sys
import json

import pandas as pd

# IFILE = "items_dump.jl"
IFILE = sys.argv[1]
OFILE = sys.argv[2]


def get_line_from_jl(ifile):
    """
    @ifile: jl file name
    generates dictionaries from each json line
    """
    with open(ifile) as f:
        for line in f:
            yield json.loads(line.strip())


def get_values_from_jl(ifile, key):
    """
    @ifile: jl file name
    @key: string
    returns list of unique values from key
    """
    values = []
    for d in get_line_from_jl(ifile):
        v = d[key]
        if isinstance(v, list):
            values.extend(v)
        else:
            values.append(v)
    return list(set(values))  # attention to order when duplicated values


def get_ocurrences(l1, l2):
    """
    returns a list with the number of
    occurrences in l1 of each element in l2
    """
    return [l2.count(el) for el in l1]


def get_matrix_from_jl(ifile, cols, ind, col):
    """
    @ifile: jl file name
    @cols: list of columns
    @ind: index name
    @col: column name
    returns dictionary with ind keys and values the number of occurrences of
    each col element in cols
    """
    matrix = {}
    for d in get_line_from_jl(ifile):
        matrix[d[ind]] = get_ocurrences(cols, d[col]) + [d["title"]]  # refactor to move "title" to args
    return matrix


def get_dataframe_from_jl(ifile, ind, col):
    cols = get_values_from_jl(ifile, col)
    matrix = get_matrix_from_jl(ifile, cols, ind, col)
    return pd.DataFrame(data=matrix, index=(cols+["title"]))  # refactor


def clean(df, index_limit, column_limit):
    """
    returns a dataframe with indexes that appear more than index_limit
    and columns more than column_limit

    It cleans indexes and columns until there is no change in shape meaning
    that the output satisfies the conditions

    CHECK: if the dataset returned is empty
    """
    shape_before = df.shape

    while True:
        df = df.loc[df.sum(axis=1) > index_limit,
                    df.sum(axis=0) > column_limit]
        if shape_before == df.shape:
            break
        else:
            shape_before = df.shape

    return df

def get_tags(series):
    return series[series > 0].index.tolist()

def df_to_json(df):
    d = []
    for index, series in df.iterrows():
        id = index
        tags = get_tags(series.iloc[:-1])
        title = series[-1]
        d.append({"id": id, "tags": tags, "title": title})
    return json.dumps(d)


if __name__ == '__main__':
    # Get data frame with rows with tags and title, and columns with ids
    df = get_dataframe_from_jl(IFILE, "id", "tags")

    # Get data frame with rows with ids, and columns with tags and title
    df = df.transpose()

    # Get id-titles series
    titles = df.iloc[:, -1]

    # Clean df based on tags frequency and jobs frequency
    df = clean(df.iloc[:, :-1], 3, 3)

    # Add titles column to df_clean (BUG: if there is a tag with name 'title'
    df = df.join(titles)

    # Get json {"id": 1243, "tags": ["java",...], "title": "Java developer"}
    js = df_to_json(df)

    # Write to file items_clean.json
    open(OFILE, "w").write(js)
