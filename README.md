# HelloFresh
Cleaning the recipe data of HelloFresh and Classifying them into Easy, Medium and Hard recipes and some aggregations on the classified data.

PROBLEM STATEMENT

Read the data file and find out the average of the total time taken to prepare all the dishes for each difficulty level.

	The input file (recipes.json) containing details of different recipes.
	The schema looks like:
	  root
	   |-- cookTime: string (nullable = true)
	   |-- datePublished: string (nullable = true)
	   |-- description: string (nullable = true)
	   |-- image: string (nullable = true)
	   |-- ingredients: string (nullable = true)
	   |-- name: string (nullable = true)
	   |-- prepTime: string (nullable = true)
	   |-- recipeYield: string (nullable = true)
	   |-- url: string (nullable = true) 


We categorize the difficulty level of a dish on the basis of totalTime which is the total time taken to get a dish ready i.e., the sum of prepTime and cookTime. cookTime and prepTime are in ISO format. For example, PT, PT1H15M, PT8H, PT30M.

The classification is done based on the below conditions.

  easy   : if total time taken is less than 30 mins
  medium : if it is not easy and total time taken is less than 60 mins
  hard   : if it is not medium and total time taken is more than 60 mins

SOLUTION

Used Spark-SQL and Created a Pyspark Script which performs 

1) Data cleaning activities like removing the prefix 'PT' from the columns cookTime and prepTime and extracting the total minutes from the resulting time strings (1H15M, 8H, 30M, etc.,)
2) Addition of new column totalTime by adding prepTime and cookTime.
3) Addition of another column difficultyLevel based on totalTime.
4) Finding the average of the total time required to prepare all dishes for each difficulty level.
