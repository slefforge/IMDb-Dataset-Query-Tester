# IMDb Dataset Query Tester

This program is designed for testing queries on the [IMDB Non-Commercial Dataset](https://developer.imdb.com/non-commercial-datasets/). 

## Prerequisites

Before running the program, ensure that the `/data` directory is populated with all the appropriate TSV files:

```
.
├── name.basics.tsv
├── title.akas.tsv
├── title.basics.tsv
├── title.crew.tsv
├── title.episode.tsv
├── title.principals.tsv
└── title.ratings.tsv
```

*Note: The files currently in the directory are truncated for testing purposes. To conduct proper testing, download and unzip the [complete versions](https://datasets.imdbws.com/).*

## Compilation

To compile the program, use the following command:

```
make
```

## Execution

Run the program using the command:

```
./testQuery
```

### Instructions

Upon launching the executable, you will be prompted to type `'y'` to execute the stored query, or `'n'` to exit the program. Typing `'y'` will execute the current query stored in `query.txt`. 

- The results of the query will be saved in `result.txt`.
- Timing information will be displayed on the standard output.

## Performance Considerations

- The IMDB Non-Commercial Dataset is quite large, so loading it initially can take a few minutes.
- If you've already loaded the dataset and would like to re-launch the program without waiting for it to reload, you can use the `--preserve` parameter as shown below:

  ```
  ./testQuery --preserve
  ```

  This will bypass reloading the dataset and instead use the `moviedb.sqlite` that already exists. 

*Important:* If you haven't generated this for the first time or have overwritten it with incomplete data, you should not use the `--preserve` parameter.
