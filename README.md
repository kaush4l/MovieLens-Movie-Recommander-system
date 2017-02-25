# MovieLens-Movie-Recommander-system
Movie recommendation system using MapReduce:
==========================
1. Movie recommendation system in map reduce is a tedious task that needs multiple map and reduce jobs to be computed in parallel.
2.Initial map-reduce job performs data cleaning that returns output as the required format. 
3. Second map-reduce job returns the list of movies rated by the selected user from the movielens dataset 
4. Third map-reduce job performs comparison with other users who has movies which are rated by both.
5. Fourth map-reduce job returns the movies which are taked from most similar user.
6. These values are in turn returned to the first stage to calculate the page rank in next iteration with updated page ranks. 10 such iterations are run to make sure that the values of each page converge.
7. In third map reduce job, the pages with top page ranks are sorted in descending order so that the highest ranked pages is on the top.
8. The final output of the mapreduce program returns suggest the movies based on the movie rating of user's history.
9. Three Arguments have to be passed while running the code : Two for input path and the other for output path.
  input1 : dataset file path
  input2 : Enter the usedId of a User to whom the recommendations are to be given 
  output : Output file path 
10. Eg: hadoop jar <name.jar> <package.class.name> <path to input file> <path to input file> <path to output file>
11. Output can be seen using: hadoop fs -cat /path/to/output/file/*
