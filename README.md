# Mytutor Semantic Interface
A python library for working with metrics on top of Lightdash APIs

## To set up Anaconda environment

Duplicate the `environment.yml.template` file and rename it to `environment.yml` and update the file variables for Snowflake credentials.

Run the following command in terminal to build an Anaconda enviornment called `analysis` -

```shell
conda env create -f environment.yml
```

Run the following command to check if the anaconda environment variable is set up correctly -

```shell
conda env config vars list 
```

## To update Anaconda environment

Run the following command in terminal to update the `analysis` Anaconda environment -

```shell
conda env update --file environment.yml --prune
```
