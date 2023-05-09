---
title: "Machine Learning prep: cleaning, encoding, and splits, oh my!" 
id: "11-machine-learning-prep"
description: "Machine Learning prep"
---
Now that we’ve gained insights and business intelligence about Formula 1 at a descriptive level, we want to extend our capabilities into prediction. We’re going to take the scenario where we censor the data. This means that we will pretend that we will train a model using earlier data and apply it to future data. In practice, this means we’ll take data from 2010-2019 to train our model and then predict 2020 data.

In this section, we’ll be preparing our data to predict the final race position of a driver.

At a high level we’ll be:

- Creating new prediction features and filtering our dataset to active drivers
- Encoding our data (algorithms like numbers) and simplifying our target variable called `position`
- Splitting our dataset into training, testing, and validation

## ML data prep

1. To keep our project organized, we’ll need to create two new subfolders in our `ml` directory. Under the `ml` folder, make the subfolders `prep` and `train_predict`.
2. Create a new file under `ml/prep` called `ml_data_prep.py`. Copy the following code into the file and **Save**.
    ```python 
    import pandas as pd

    def model(dbt, session):
        # dbt configuration
        dbt.config(packages=["pandas"])

        # get upstream data
        fct_results = dbt.ref("fct_results").to_pandas()

        # provide years so we do not hardcode dates in filter command
        start_year=2010
        end_year=2020

        # describe the data for a full decade
        data =  fct_results.loc[fct_results['RACE_YEAR'].between(start_year, end_year)]

        # convert string to an integer
        data['POSITION'] = data['POSITION'].astype(float)

        # we cannot have nulls if we want to use total pit stops 
        data['TOTAL_PIT_STOPS_PER_RACE'] = data['TOTAL_PIT_STOPS_PER_RACE'].fillna(0)

        # some of the constructors changed their name over the year so replacing old names with current name
        mapping = {'Force India': 'Racing Point', 'Sauber': 'Alfa Romeo', 'Lotus F1': 'Renault', 'Toro Rosso': 'AlphaTauri'}
        data['CONSTRUCTOR_NAME'].replace(mapping, inplace=True)

        # create confidence metrics for drivers and constructors
        dnf_by_driver = data.groupby('DRIVER').sum()['DNF_FLAG']
        driver_race_entered = data.groupby('DRIVER').count()['DNF_FLAG']
        driver_dnf_ratio = (dnf_by_driver/driver_race_entered)
        driver_confidence = 1-driver_dnf_ratio
        driver_confidence_dict = dict(zip(driver_confidence.index,driver_confidence))

        dnf_by_constructor = data.groupby('CONSTRUCTOR_NAME').sum()['DNF_FLAG']
        constructor_race_entered = data.groupby('CONSTRUCTOR_NAME').count()['DNF_FLAG']
        constructor_dnf_ratio = (dnf_by_constructor/constructor_race_entered)
        constructor_relaiblity = 1-constructor_dnf_ratio
        constructor_relaiblity_dict = dict(zip(constructor_relaiblity.index,constructor_relaiblity))

        data['DRIVER_CONFIDENCE'] = data['DRIVER'].apply(lambda x:driver_confidence_dict[x])
        data['CONSTRUCTOR_RELAIBLITY'] = data['CONSTRUCTOR_NAME'].apply(lambda x:constructor_relaiblity_dict[x])

        #removing retired drivers and constructors
        active_constructors = ['Renault', 'Williams', 'McLaren', 'Ferrari', 'Mercedes',
                            'AlphaTauri', 'Racing Point', 'Alfa Romeo', 'Red Bull',
                            'Haas F1 Team']
        active_drivers = ['Daniel Ricciardo', 'Kevin Magnussen', 'Carlos Sainz',
                        'Valtteri Bottas', 'Lance Stroll', 'George Russell',
                        'Lando Norris', 'Sebastian Vettel', 'Kimi Räikkönen',
                        'Charles Leclerc', 'Lewis Hamilton', 'Daniil Kvyat',
                        'Max Verstappen', 'Pierre Gasly', 'Alexander Albon',
                        'Sergio Pérez', 'Esteban Ocon', 'Antonio Giovinazzi',
                        'Romain Grosjean','Nicholas Latifi']

        # create flags for active drivers and constructors so we can filter downstream              
        data['ACTIVE_DRIVER'] = data['DRIVER'].apply(lambda x: int(x in active_drivers))
        data['ACTIVE_CONSTRUCTOR'] = data['CONSTRUCTOR_NAME'].apply(lambda x: int(x in active_constructors))
        
        return data
    ```
3. As usual, let’s break down what we are doing in this Python model:
    - We’re first referencing our upstream `fct_results` table and casting it to a pandas dataframe.
    - Filtering on years 2010-2020 since we’ll need to clean all our data we are using for prediction (both training and testing).
    - Filling in empty data for `total_pit_stops` and making a mapping active constructors and drivers to avoid erroneous predictions
        - ⚠️ You might be wondering why we didn’t do this upstream in our `fct_results` table! The reason for this is that we want our machine learning cleanup to reflect the year 2020 for our predictions and give us an up-to-date team name. However, for business intelligence purposes we can keep the historical data at that point in time. Instead of thinking of one table as “one source of truth” we are creating different datasets fit for purpose: one for historical descriptions and reporting and another for relevant predictions.
    - Create new features for driver and constructor confidence. This metric is created as a proxy for understanding consistency and reliability. There are more aspects we could consider for this project, such as normalizing the driver confidence by the number of races entered, but we'll keep it simple.
    - Generate flags for the constructors and drivers that were active in 2020
4. Execute the following in the command bar:
    ```bash
    dbt run --select ml_data_prep
    ```
6. Let’s look at the preview of our clean dataframe after running our `ml_data_prep` model:
  <Lightbox src="/img/guides/dbt-ecosystem/dbt-python-snowpark/11-machine-learning-prep/1-completed-ml-data-prep.png" title="What our clean dataframe fit for machine learning looks like"/>

## Covariate encoding

In this next part, we’ll be performing covariate encoding. Breaking down this phrase a bit, a *covariate* is a variable that is relevant to the outcome of a study or experiment, and *encoding* refers to the process of converting data (such as text or categorical variables) into a numerical format that can be used as input for a model. This is necessary because most machine learning algorithms can only work with numerical data. Algorithms don’t speak languages, have eyes to see images, etc. so we encode our data into numbers so algorithms can perform tasks by using calculations they otherwise couldn’t.

🧠 We’ll think about this as : “algorithms like numbers”.

1. Create a new file under `ml/prep` called `covariate_encoding.py` copy the code below and save.
    ```python
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import StandardScaler,LabelEncoder,OneHotEncoder
    from sklearn.linear_model import LogisticRegression

    def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas","numpy","scikit-learn"])

    # get upstream data
    data = dbt.ref("ml_data_prep").to_pandas()

    # list out covariates we want to use in addition to outcome variable we are modeling - position
    covariates = data[['RACE_YEAR','CIRCUIT_NAME','GRID','CONSTRUCTOR_NAME','DRIVER','DRIVERS_AGE_YEARS','DRIVER_CONFIDENCE','CONSTRUCTOR_RELAIBLITY','TOTAL_PIT_STOPS_PER_RACE','ACTIVE_DRIVER','ACTIVE_CONSTRUCTOR', 'POSITION']]
    
    # filter covariates on active drivers and constructors
    # use fil_cov as short for "filtered_covariates"
    fil_cov = covariates[(covariates['ACTIVE_DRIVER']==1)&(covariates['ACTIVE_CONSTRUCTOR']==1)]

    # Encode categorical variables using LabelEncoder
    # TODO: we'll update this to both ohe in the future for non-ordinal variables! 
    le = LabelEncoder()
    fil_cov['CIRCUIT_NAME'] = le.fit_transform(fil_cov['CIRCUIT_NAME'])
    fil_cov['CONSTRUCTOR_NAME'] = le.fit_transform(fil_cov['CONSTRUCTOR_NAME'])
    fil_cov['DRIVER'] = le.fit_transform(fil_cov['DRIVER'])
    fil_cov['TOTAL_PIT_STOPS_PER_RACE'] = le.fit_transform(fil_cov['TOTAL_PIT_STOPS_PER_RACE'])

    # Simply target variable "position" to represent 3 meaningful categories in Formula1
    # 1. Podium position 2. Points for team 3. Nothing - no podium or points!
    def position_index(x):
        if x<4:
            return 1
        if x>10:
            return 3
        else :
            return 2

    # we are dropping the columns that we filtered on in addition to our training variable
    encoded_data = fil_cov.drop(['ACTIVE_DRIVER','ACTIVE_CONSTRUCTOR'],1)
    encoded_data['POSITION_LABEL']= encoded_data['POSITION'].apply(lambda x: position_index(x))
    encoded_data_grouped_target = encoded_data.drop(['POSITION'],1)

    return encoded_data_grouped_target
    ```
2. Execute the following in the command bar:
    ```bash
    dbt run --select covariate_encoding
    ```
3. In this code we are using [Scikit-learn](https://scikit-learn.org/stable/), “sklearn” for short, is an extremely popular data science library. We’ll be using Sklearn for both preparing our covariates and creating models (our next section). Our dataset is pretty small data so we are good to use pandas and `sklearn`. If you have larger data for your own project in mind, consider `dask` or `category_encoders`.
4. Breaking our code down a bit more:
    - We’re selecting a subset of variables that will be used as predictors for a driver’s position.
    - Filter the dataset to only include rows using the active driver and constructor flags we created in the last step.
    - The next step is to use the `LabelEncoder` from scikit-learn to convert the categorical variables `CIRCUIT_NAME`, `CONSTRUCTOR_NAME`, `DRIVER`, and `TOTAL_PIT_STOPS_PER_RACE` into numerical values.
    - To simplify the classification and improve performance, we are creating a new variable called `POSITION_LABEL` from our original position variable with in Formula 1 with 20 total positions. This new variable has a specific meaning: those in the top 3 get a “podium” position, those in the top 10 get points that add to their overall season total, and those below the top 10 get no points. The original position variable is being mapped to position_label in a way that assigns 1, 2, and 3 to the corresponding places.
    - Drop the active driver and constructor flags since they were filter criteria and additionally drop our original position variable.

## Splitting into training and testing datasets

In this step, we will create dataframes to use for training and prediction. We’ll be creating two dataframes 1) using data from 2010-2019 for training, and 2) data from 2020 for new prediction inferences. We’ll create variables called `start_year` and `end_year` so we aren’t filtering on hardcasted values (and can more easily swap them out in the future if we want to retrain our model on different timeframes).

1. Create a file called `train_test_dataset.py` copy and save the following code:
    ```python 
    import pandas as pd

    def model(dbt, session):

        # dbt configuration
        dbt.config(packages=["pandas"], tags="train")

        # get upstream data
        encoding = dbt.ref("covariate_encoding").to_pandas()

        # provide years so we do not hardcode dates in filter command
        start_year=2010
        end_year=2019

        # describe the data for a full decade
        train_test_dataset =  encoding.loc[encoding['RACE_YEAR'].between(start_year, end_year)]

        return train_test_dataset
    ```

2. Create a file called `hold_out_dataset_for_prediction.py` copy and save the following code below. Now we’ll have a dataset with only the year 2020 that we’ll keep as a hold out set that we are going to use similar to a deployment use case.
    ```python 
    import pandas as pd

    def model(dbt, session):
        # dbt configuration
        dbt.config(packages=["pandas"], tags="predict")

        # get upstream data
        encoding = dbt.ref("covariate_encoding").to_pandas()
        
        # variable for year instead of hardcoding it 
        year=2020

        # filter the data based on the specified year
        hold_out_dataset =  encoding.loc[encoding['RACE_YEAR'] == year]
        
        return hold_out_dataset
    ```
3. Execute the following in the command bar:
    ```bash
    dbt run --select train_test_dataset hold_out_dataset_for_prediction
    ```
    To run multiple models by name, we can use the *space* syntax [syntax](/reference/node-selection/syntax) between the model names. 
4. **Commit and sync** our changes to keep saving our work as we go using `ml data prep and splits` before moving on.

👏 Now that we’ve finished our machine learning prep work we can move onto the fun part &mdash; training and prediction!