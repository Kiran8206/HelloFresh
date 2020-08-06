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


We categorise the difficulty level of a dish on the basis of totalTime which is the total time taken to get a dish ready i.e., the sum of prepTime and cookTime. cookTime and prepTime are in ISO format. The classification is done based on the below conditions.

  easy   : if total time taken is less than 30 mins
  medium : if it is not easy and total time taken is less than 60 mins
  hard   : if it is not medium and total time taken is more than 60 mins
