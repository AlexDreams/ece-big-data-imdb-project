# ECE Big Data Processing – IMDb project report

## 1. Data preparation

### Results

The IMDb snapshot used in this project contains a total of seven datasets. The first step was to measure missing values in the original data, before applying any cleaning rules. IMDb represents missing fields with `\N` (no value provided). However we found no fields that were empty strings or contained only spaces.

The five columns with the highest missing-value percentages are:

| Table.column | Missing (%) |
| ------------ | -----------: |
| `title.akas.attributes` | 99.472 |
| `title.basics.endYear` | 98.732 |
| `name.basics.deathYear` | 98.320 |
| `name.basics.birthYear` | 95.658 |
| `title.principals.job` | 80.691 |

We figured these percentages do not all indicate a data-quality failure. For example, `endYear` is generally not applicable to films, and `deathYear` is unavailable for people who are still alive. Another example is that when a person’s birthYear is missing, we cannot calculate their age, so fewer people can be included in age-based analyses.

False or non-sense data can be just as harmful as missing values as they can break the system or biais the results. Therefore, we also ran 31 checks for inconsistent values, duplicate keys and broken references between tables. The non-zero findings included:

| Table | Check | Rows | Share of table |
| ----- | ----- | ---: | -------------: |
| `name.basics` | Death before birth | 27 | 0.0002% |
| `name.basics` | Lifespan over 120 years | 2 | 0.0000% |
| `name.basics` | No death year, birth before 1906 | 2,793 | 0.0177% |
| `title.basics` | Runtime at or below zero | 4 | 0.0000% |
| `title.basics` | Runtime over 24 hours | 209 | 0.0016% |
| `title.basics` | Start year after 2026 | 5,213 | 0.0404% |
| `title.basics` | Movies without a genre | 77,821 | 0.6032% |
| `title.principals` | Person ID without a matching person | 7,174 | 0.0070% |
| `title.principals` | Credited more than two years after death | 229,691 | 0.2240% |
| `title.principals` | Credited before birth | 3,110 | 0.0030% |
| `title.crew` | Director ID without a matching person | 420 | 0.0044% |
| `title.crew` | Writer ID without a matching person | 1,146 | 0.0075% |
| `title.episode` | Parent title not found | 5 | 0.0001% |
| `title.akas` | Title ID not found | 33 | 0.0001% |

The checks identify values that may be incorrect, but we review them before deciding whether they should be changed or removed. For example, a person may be credited after their death because archival footage was used, or before a film's release because they contributed to a later re-release. They are also limits linked to IMDb that cannot represent consistently historical birth and death years.

After all the cleaning processes, the row counts we end up with are:

| Silver table | Rows before | Rows after |
| ------------ | ----------: | ---------: |
| `name_basics` | 15,768,361 | 15,768,361 |
| `title_basics` | 12,902,085 | 12,902,085 |
| `title_ratings` | 1,726,141 | 1,726,141 |
| `title_principals` | 102,539,871 | 102,532,697 |
| `title_episode` | 9,980,994 | 9,980,989 |
| `title_akas` | 59,957,982 | 59,957,949 |
| `title_directors` | 9,569,644 | 9,569,224 |
| `title_writers` | 15,203,836 | 15,202,690 |

We removed some rows from the fact and link tables, that corresponded to references with no matching parent: 7,174 principal records, 5 episodes, 33 alternate-title records, 420 director links and 1,146 writer links.

### Methodological explanation

#### From raw files to typed tables

The data is distributed in a three layers. We converted the downloaded IMDb TSV files to Parquet in the bronze layer while keeping their fields as text so that a failed type conversion would not discard the original value. Missing markers (`\N`) were read as null. We then converted the fields needed by the pipeline to their expected types and normalized blank strings to null. During quality checks, the original text was also retained for fields being cast, making it possible to distinguish real missing value from values that were present but could not be converted.

The bronze data was our "backup" as it remained unchanged and gives us a reference point if a cleaning rule needs to be revised. As mentioned before the project gave us the chance to work with Parquet which was more convenient for repeated Spark processing than the original compressed TSV files. The only dowside was that the project had to store another copy of the data.

#### Measuring missing values and checking quality

To produce our final missing-value report, we started to calculate from bronze data, before cleaning, so it describes the downloaded dataset rather than the result of our own transformations. A value counted as missing if it was null or blank. For efficiency, Spark aggregates all columns of a table together, avoiding a separate full scan for every column, so our large tables such as `title.principals` were easier to deal with.

The quality checks cover value ranges and timelines, duplicate identifiers, and possible mismatching references between tables. For example, the checks look for impossible runtimes, dates that do not fit a person's timeline, and IDs in credit or episode tables that have no corresponding person or title. To resolve the "credits after death" problem, we choose to set up a two-year allowance, since a film can still be released if the filming is complete (we excluded the Archive-related credit categories in that check). The checks are indicative as a flagged row is not automatically treated as an error, since some apparent inconsistencies have legitimate explanations as we saw multiple times.

#### Rules used for the silver layer

For the cleaning step, we had to apply a small set of explicit rules like: failed casts and blank text become null or implausible dates or runtimes are nulled rather than guessed for example. When a value is changed this way, the row retains a `dq_flags` entry describing the issue which is usefull for keeping the system clear and understandable at any time. 

Here are some various ways the system work, depending on if we can bypass the issue or the possible explanations: Birth and death years are both nulled when death precedes birth (no possible explanation) but we did not remove the future-dated titles detected by the checks, because they may be announced releases, and titles without a genre remain available with an empty genre list.

When the forign key of a record key has no corresponding person or title, it is removed from the related fact or link table as it cannot be joined to a valid parent and the small deletion counts concords with the orphan counts from the quality report. By contrast, timeline anomalies in `title.principals` are retained with flags because the example records suggest that some of them are valid credits, such as archival appearances or music added to an older film. Downstream analyses can then exclude flagged records when their question requires a consistent timeline.

The duplicate-removal rules are applied to the relevant identifiers even though the checks found no duplicate keys in this snapshot; this protects the pipeline if a later dataset version contains them. Ratings are kept only when they are in the 1–10 range and have at least one vote. The comma-separated director and writer fields are expanded into link tables, so each title-person relationship can be joined directly rather than parsed from a list in one cell. Episodes are kept only if both the episode and its parent exist and the parent is a series.

Our general goal was to favor traceability over aggressive correction that could cause data compromisation.  Questionable data is either marked or set to unknown instead of being replaced with made up values. The trade-off is that nulling dates can lose information, especially for historical figures whose dates are difficult to represent, and later analyses must decide how to handle the flags. We also thought about dropping every flagged row, as a more radical approach, but that would also be risking discarding legitimate records.

## 2. Data exploration

This section answers the 8 exploratory questions on the topic, presenting the results graphically in a notebook using data from the “silver” layer (data from the IMDb tables that have been cleaned during data preparation).
The code is located in src/exploration, with the questions.py file containing the code for all 8 questions.
The complete results in CSV format are located in output/exploration, and the notebook with the tables and charts is located in notebooks/exploration.ipynb.
To rerun the questions and refresh the notebook, simply run this command: docker exec imdb-spark python -m src.exploration.questions


### Results

#### Runtime outliers by genre (q1)

| Genre | Films | Avg runtime (min) | Std dev (min) | Outliers | Longer | Shorter |
| ----- | ----: | ----------------: | ------------: | -------: | -----: | ------: |
| Drama | 196,414 | 95.9 | 27.5 | 5,716 | 5,716 | 0 |
| Documentary | 115,845 | 76.6 | 30.2 | 2,145 | 2,145 | 0 |
| Comedy | 96,482 | 93.1 | 22.3 | 4,628 | 3,586 | 1,042 |
| Romance | 42,414 | 99.1 | 25.1 | 2,042 | 1,813 | 229 |
| Action | 40,300 | 101.1 | 31.3 | 1,146 | 1,146 | 0 |
| Crime | 34,565 | 95.5 | 25.6 | 1,174 | 1,165 | 9 |
| Thriller | 34,013 | 96.1 | 21.7 | 2,084 | 1,386 | 698 |
| Horror | 30,667 | 88.6 | 22.3 | 700 | 696 | 4 |
| Adventure | 24,031 | 93.0 | 30.6 | 633 | 633 | 0 |
| Mystery | 16,457 | 93.9 | 24.8 | 449 | 446 | 3 |

This table shows only the top 10 out of 27 genres

#### Top 3 genres per year (q2)

| Year | #1 | #2 | #3 |
| ---: | -- | -- | -- |
| 1980 | Drama | Adventure | Horror |
| 1985 | Comedy | Adventure | Drama |
| 1990 | Drama | Comedy | Crime |
| 1995 | Drama | Crime | Comedy |
| 2000 | Drama | Comedy | Adventure |
| 2005 | Drama | Comedy | Action |
| 2010 | Drama | Action | Adventure |
| 2015 | Drama | Action | Adventure |
| 2020 | Drama | Comedy | Action |
| 2025 | Drama | Action | Comedy |

This table shows the top 3 genres only for one out of every five years so as not to clutter the report.

#### Top 3 genres compared to the rest (q3)

| Group | Avg rating | Avg votes per film | Avg runtime (min) |
| ----- | ---------: | -----------------: | ----------------: |
| Top 3 | 6.04 | 7,895 | 100.4 |
| Rest | 6.14 | 7,047 | 93.7 |

| Genre | Years in top 3 (out of 46) |
| ----- | -------------------------: |
| Drama | 46 |
| Comedy | 34 |
| Action | 29 |
| Adventure | 16 |
| Crime | 9 |
| Horror | 2 |
| Thriller | 1 |
| Sci-Fi | 1 |

| Year | Top 3 vote share | Top 3 films | Rest films | Votes per film (top 3) | Votes per film (rest) |
| ---: | ---------------: | ----------: | ---------: | ---------------------: | --------------------: |
| 1980 | 44.4% | 1,355 | 2,304 | 5,686 | 4,192 |
| 1990 | 45.6% | 2,159 | 2,139 | 6,443 | 7,765 |
| 2000 | 45.0% | 2,434 | 2,856 | 11,637 | 12,135 |
| 2010 | 40.0% | 4,099 | 8,329 | 10,742 | 7,937 |
| 2020 | 44.8% | 5,444 | 8,522 | 4,241 | 3,336 |

This table presents the results for one year out of every 10

Based on these three tables of results, we can see that the top three genres account for between 38% and 58% of the votes, depending on the year, but they also represent, on average, 44% of the films produced that year.
However, when it comes to the average ratings by genre, the top three genres score slightly lower than the rest.
We can also see that certain genres appear more frequently than others, particularly drama, which is present in all 46 years of the study.
The top three genres received more votes per film in only 26 out of 46 years, which shows that their ranking is primarily due to the number of films produced in those genres, rather than the fact that films in those genres are watched more frequently on an individual basis.

#### One-hit directors (q4)

| Director | Other movies | Avg rating (others) | Hit | Year | Hit rating |
| -------- | -----------: | ------------------: | --- | ---: | ---------: |
| Wai Keung Lau | 43 | 5.72 | Infernal Affairs | 2002 | 8.0 |
| Mahesh Bhatt | 36 | 5.68 | Saaransh | 1984 | 8.1 |
| Shaji Kailas | 35 | 5.33 | Aaram Thamburan | 1997 | 8.1 |
| Puri Jagannadh | 33 | 5.52 | Pokiri | 2006 | 8.0 |
| Sundar C. | 25 | 5.68 | Anbe Sivam | 2003 | 8.6 |
| Suresh Krishna | 24 | 5.98 | Baasha | 1995 | 8.2 |
| Robert Rodriguez | 23 | 5.9 | Sin City | 2005 | 8.0 |
| Rabi Kinagi | 23 | 5.76 | Suna Panjuri | 1995 | 9.0 |
| Smeep Kang | 22 | 5.84 | Carry on Jatta | 2012 | 8.3 |
| John G. Avildsen | 21 | 5.78 | Rocky | 1976 | 8.1 |

A total of 188 directors meet the criteria for this question. 

#### Declining TV series (q5)

| Series | Start year | Full seasons (≥10 ep.) | Last season | Best episode | Worst episode |
| ------ | ---------: | ---------------------: | ----------: | -----------: | ------------: |
| The Queen of Flow | 2018 | 3 | 3 | 9.8 | 1.1 |
| One Punch Man | 2015 | 3 | 3 | 9.5 | 1.6 |
| Celebrity Substitute | 2024 | 3 | 3 | 9.6 | 2.3 |
| House of Cards | 2013 | 5 | 6 | 9.5 | 2.5 |
| Family Law | 1999 | 3 | 3 | 9.4 | 2.7 |
| The Gumby Show | 1956 | 3 | 3 | 8.1 | 1.4 |
| All Dogs Go to Heaven: The Series | 1996 | 3 | 3 | 9.2 | 2.6 |
| FRO Thizzle Reviews | 2022 | 5 | 5 | 10.0 | 3.6 |
| Margarita | 2024 | 3 | 3 | 9.4 | 3.1 |
| Soy Luna | 2016 | 4 | 4 | 9.1 | 2.8 |

The results show that 639 series match the filters.
There is no minimum number of votes per episode, so series with very few votes are included here as well.

#### Success across languages (q6)

| Step | Films |
| ---- | ----: |
| Successful films (rating ≥ 7.0, ≥ 1,000 votes) | 14,156 |
| With an original language on Wikidata | 12,656 |
| With a single original language (used) | 11,414 (80.6%) |

| Actor | Languages | Language codes | Successful films |
| ----- | --------: | -------------- | ---------------: |
| Atul Kulkarni | 6 | hi, kn, ml, mr, ta, te | 12 |
| Bruno Ganz | 6 | de, el, en, is, it, sv | 11 |
| Amrish Puri | 5 | en, hi, ml, ta, te | 25 |
| Max von Sydow | 5 | en, fr, it, no, sv | 23 |
| Mukesh Rishi | 5 | hi, kn, mr, ta, te | 17 |
| Genelia Deshmukh | 5 | hi, ml, mr, ta, te | 12 |
| Dhritiman Chatterjee | 5 | bn, en, hi, ml, ta | 12 |
| Franco Nero | 5 | de, en, es, it, sr | 10 |
| Rohini Hattangadi | 5 | en, hi, mr, ta, te | 10 |
| Jaya Prada | 5 | hi, kn, ml, ta, te | 8 |

These data show that actors who are successful in multiple languages are quite rare; there are only 2,240 who are successful in two languages and 326 who are successful in three languages.
It is also evident that many actors who are successful in multiple languages are Indian; for example, among those successful in three or more languages, 41% of the entries consist solely of Indian languages.

#### Actors whose popularity peaked early (q7)

| Actor | Career | Movies | Peak year | Peak rating | Avg 1st half | Avg 2nd half |
| ----- | ------ | -----: | --------: | ----------: | -----------: | -----------: |
| Veronica Lake | 1941–1970 | 20 | 1941 | 7.9 | 6.42 | 2.50 |
| Mahesh Anand | 1988–2019 | 33 | 1991 | 7.4 | 5.47 | 1.90 |
| Kanakalatha | 1986–2017 | 16 | 1993 | 7.9 | 6.48 | 2.93 |
| Robert Woods | 1966–2016 | 22 | 1966 | 6.1 | 5.12 | 1.70 |
| Artur Rolén | 1918–1967 | 20 | 1918 | 7.2 | 7.20 | 3.84 |
| Aleksey Batalov | 1954–2007 | 16 | 1957 | 8.3 | 7.35 | 4.05 |
| Ken Maynard | 1931–1970 | 19 | 1944 | 6.7 | 5.97 | 2.70 |
| Norman Wisdom | 1953–2002 | 18 | 1963 | 6.9 | 6.31 | 3.20 |
| Saroja Devi B. | 1958–2023 | 16 | 1977 | 8.9 | 8.01 | 5.07 |
| Sadek Bachchu | 1994–2020 | 16 | 1997 | 8.2 | 8.05 | 5.17 |

We can see that there are 4,495 actors who meet the criteria and that the median difference between the first half and the second half of their careers is only 0.55 points, which represents a slight decline.

#### Multi-talented professionals (q8)

| Career profile | Avg rating (multiple roles) | Titles | Avg rating (single role) | Titles |
| -------------- | --------------------------: | -----: | -----------------------: | -----: |
| actor + director | 5.69 | 897 | 5.98 | 9,839 |
| actor + writer | 5.64 | 3,458 | 5.84 | 18,265 |
| actor + director + writer | 5.87 | 11,309 | 5.88 | 20,894 |
| director + writer | 6.00 | 40,809 | 5.97 | 30,680 |

| People compared to themselves | Mean gap (multiple − single) | Better when combining roles |
| ----------------------------: | ---------------------------: | --------------------------: |
| 8,473 | +0.048 | 53.1% |

| Person | Titles with multiple roles | Avg (multiple) | Avg (single) |
| ------ | -------------------------: | -------------: | -----------: |
| Woody Allen | 54 | 6.89 | 5.69 |
| Satyajit Ray | 31 | 7.87 | 6.83 |
| Charles Chaplin | 17 | 7.55 | 6.53 |
| Quentin Tarantino | 15 | 7.71 | 6.68 |
| Clint Eastwood | 25 | 6.91 | 6.83 |
| Spike Lee | 19 | 6.43 | 6.92 |

The first table shows that acting in a film in addition to directing or writing it results in slightly lower ratings (-0.29 for an actor-director, for example).
The second table shows the effect on an individual basis, and the gap is almost zero (+0.048), which means that taking on multiple roles makes a person neither better nor worse.

### Methodological explanation


#### Runtime outliers by genre (q1)

**Method**
First, we created a function called all_feature_movies() to retrieve all titles with the “movie” type that are longer than 40 minutes, regardless of whether they have a rating or not. Initially, I had only kept movies that had a rating, but since the question didn’t specify that they had to be rated, I removed that condition, which increased the number of movies in the results.

We then pass all these movies through a function called movie_genres(), which “breaks down” the movies’ genres—that is, if a movie has three different genres, for example, it will be listed in three separate rows,
one for each genre of the movie.

Based on this genre filter, we can then group all the genres one by one and calculate statistics such as the number of movies in each genre, the average runtime of movies in that genre, and their standard deviation.

Once this data by genre has been retrieved, we can calculate what’s called the z-score for each movie within each genre. The z-score indicates the number of standard deviations by which a value lies from the mean of the data. Once calculated, this z-score allows us to identify which movie durations differ from those of other movies in that genre; we determine whether the duration is different by comparing their z-scores, and if the absolute value of the z-score is greater than 2, then the movie is considered different.

**Why**

We’ve designated it this way for several reasons:

First, the selection of films longer than 40 minutes is intended to distinguish between films considered feature-length (over 40 minutes) and short films (under 40 minutes). I also included films that aren’t rated—something I hadn’t done initially—since a rating has no bearing on a film’s runtime.

Next, regarding the decision to use the z-score, there are several reasons. First, each genre will have a different average duration and standard deviation for duration, so it wouldn’t make sense to set a fixed threshold and apply it to every genre. Furthermore, the z-score allows us to obtain an adaptive result for each genre, and by applying a fixed threshold based on that result, it’s easier to identify films that stand out from the rest.

Finally, I made sure that a film could be counted in different genres—not just its primary genre—because we’re analyzing by genre, and it wouldn’t make sense to exclude the film’s other genres.

**Alternatives and trade-offs**

The standard deviation—but especially the mean—is sensitive to outliers. Some films in these genres are 24 hours long, which significantly inflates the mean and can potentially skew the z-score, making it more
difficult to distinguish between different films using the z-score. Instead, we could have used the median and the interquartile range, which are less sensitive to outliers.

There are also genres with relatively few films, meaning their averages are unreliable for calculating the z-score.

#### Top 3 genres per year (q2)

**Method**

In this question, we need to identify the three most popular genres each year from 1980 to 2025.

To do this, as in the first question, we retrieved the list of movies longer than 40 minutes and then ran it through our function to split each movie into multiple lines, one for each
different genre in which it is listed. The difference this time is that we need to consider popularity, so we must keep only the movies that have a rating in order to assess their popularity.

We defined the popularity of a genre as the sum of the votes for the movies in that genre. Once this popularity was defined, all we had to do was sort each genre in descending order by popularity and display the top 3 genres for each year.

**Why**

We chose to measure popularity based on the total number of votes for films in a given genre; although we had considered other approaches, we felt this one was the most accurate. We had also considered measuring popularity based on
the average rating rather than the total number of votes, but we ultimately decided that this method would reflect the film’s quality rather than its popularity. The same applies to another idea, which was to measure the number of films; however, that would have reflected production volume rather than popularity.
Ultimately, we chose to measure popularity relative to the total number of votes, since this is the best way to gauge the level of engagement generated by films within a genre—which we felt was a better indicator of a genre’s popularity.

**Alternatives and trade-offs**

The biggest problem with our method is that the genres that produce the most films will inevitably receive more votes and will therefore be deemed more popular by our method. An alternative might have been to take into account the average votes per film. This would divide the total by the number of films, meaning that a genre would no longer be considered the most popular simply because it has produced the most films.

#### Top 3 genres compared to the rest (q3)

**Method**

For this question, we used the results from the previous question to compare the top 3 most popular genres against the others.
We therefore compare the top 3 with the rest of the genres in terms of the number of films, total votes, share of votes for the year, votes per film, as well as other metrics such as the average rating and average runtime for each group.
Once these comparisons were made, we stored them in three separate CSV files: one for year-by-year comparisons, another for comparisons over the entire period, and a third for the number of years each genre spent in the top three.

**Why**

We selected various criteria for these comparisons to gain an overview of the share held by the top 3 in the film industry compared to other genres each year.
The criteria are therefore the share of votes, to illustrate the concentration of popularity; votes per film, to isolate the volume effect of a genre and gauge people’s actual interest in each film; and, finally, ratings and runtime, which show whether popular genres differ in quality and format.

**Alternatives and trade-offs**

The main limitation of this model is that, because a movie with multiple genres is counted multiple times (once for each genre), it appears multiple times in the statistics. If, for example, one of its genres is in the top 3 and the other is not, it will count toward both categories. An alternative could be to classify a movie as belonging to only one genre (its primary genre), but this would result in the loss of a great deal of information, particularly regarding all the genres that serve as secondary genres.

#### One-hit directors (q4)

**Method**

In this question, we need to list the directors who have made one film rated 8 or higher and several others with an average rating of 6 or lower.

To do this, we first compiled a list of film-director pairs—that is, a list of all the films each director has made, for every director. The films included in this list are those with at least 100 votes to ensure consistency between the rating and the number of votes.

From this list, we were able to determine the number of films each director had made and calculate the number of films they had directed that received a rating of 8 or higher. Next, we took all of their other films rated below 8 and calculated the average rating of all those films.

Finally, we applied filters: the director must have exactly one film rated 8 or higher, so if the calculated number is greater than 1, we exclude that director. The same applies to films rated 6 or lower—there must be multiple such films, so we set a filter requiring at least two other films whose average rating is 6 or lower.

**Why**

For this question, we applied several filters; the first is to include only films with at least 100 votes. We made this choice to ensure consistency between the number of votes and the rating assigned, so that we wouldn’t end up with films rated 9 but with only 5 votes.

The other two filters were applied to meet the criteria of the question: each director must have only one film rated 8 or higher, so we had to ensure that this number is strictly equal to 1 for each director.
At the same time, they must have several other films with an average rating of 6 or lower, which led us to eliminate all those who did not meet this requirement.

**Alternatives and Trade-offs**

The filter requiring a film to have at least 100 votes changes our results; it removes some directors and brings others to the forefront. To obtain an even more robust list of “one-hit directors,” we could also require a minimum number of films directed in their career to avoid including new directors who haven’t made many films yet.

#### Declining TV series (q5)

**Method**

For this question, we first retrieved the season-episode pairs, which gave us a list of episodes along with the season number to which they are linked.

Based on that, we were able to count the number of seasons as well as the number of episodes per season, and keep only series with at least 3 seasons of 10 or more episodes.

Once that was done, we had to join the data with the ratings table to obtain the ratings for each episode. These ratings allowed us to apply the final filters and thus determine whether the highest-rated episode occurred during the first two seasons and whether the lowest-rated episode occurred during the last season. To determine this—for example, to verify that the worst episode is in the final season—we compared the worst episode of the final season with the worst episode of the entire series; if they had the same rating, then the worst episode was indeed in the final season; otherwise, it was not. We used the same logic to verify whether the best episode is in the first two seasons by comparing the best episode of the series with the best episode of the first two seasons.

**Why**

We included series with at least 10 episodes across 3 seasons, so at least 3 seasons must have 10 or more episodes. Not all of these episodes need to be rated; as long as the season has 10 episodes, it is included in the calculation even if the episodes in that season are not rated.
We’ve also allowed for ties. For example, if an episode is rated a 4 in both the first and last seasons, it will be counted as a series where the worst episode is in the last season.

**Alternatives and trade-offs**

Currently, there is no minimum number of votes per episode; one alternative could be to set a minimum number of votes per episode to make the list of returned series more robust, even if it results in fewer series.
Similarly, ongoing seasons are treated as the latest season. An alternative could be to exclude ongoing seasons and include only completed ones.


#### Success across languages (q6)

**Method**

To identify successful actors and actresses across multiple languages, we first need to define what constitutes a successful movie. In our case, we decided that a successful movie is one with a rating of 7.0 or higher and at least 1,000 votes.

Next, we had to determine the film’s original language, which is not included in the IMDb dataset. We used Wikidata because a large number of the films listed there include the original language as well as an IMDb ID, allowing us to link the two databases. Once we retrieved the list of movies with their original languages, we kept only those with a single original language to prevent a movie from automatically counting toward two different languages.

After that, we were able to join the actors and movies to determine the list of movies for each actor and then count the number of different languages and blockbuster movies in which they starred.

**Why**

We decided to define a hit movie based on two criteria: a rating (which must be 7.0 or higher) and the number of votes (at least 1,000). This was to avoid including movies with extremely high ratings but very few reviews, since such movies cannot really be considered hits in that case.

We also decided to retrieve the data from the original language, since that seemed like the simplest approach to us—given that they have an identifier that allowed us to link them directly to our IMDb database without the risk of having to go through the title and making a mistake between two movies with the same title.

**Alternatives and Trade-offs**

Some of the films we considered to be hits were lost when we imported the data from Wikidata, since some have no registered languages while others have multiple ones. In total, about 19% of the hit films were lost.
We also encountered an error regarding the Chinese language: Wikidata records some of these films as “Chinese” and others as “Mandarin,” even though they are the same language, which skews some results.
An alternative would have been to infer the original language based on the film’s country of production, but this would have been imprecise, particularly for countries like India that have multiple languages.

#### Actors whose popularity peaked early (q7)

**Method**

For this question, we had to find actors with careers spanning 20 years or more, who had appeared in at least 15 films, and whose best film was made early in their careers.

First, we collected the data we needed for each actor—namely, their films (only those with at least 100 votes), the first and last years of their careers, the number of films they’ve made, and their highest-rated film.
Once this data was collected, we could apply filters: first, we filtered to get all actors with 20-year careers (a simple subtraction of their starting year from their ending year) as well as those with at least 15 films to their credit.
To determine whether an actor’s best film was made early in their career, we established two criteria: the first was to look at their highest-rated film and determine whether it fell within the first third of their career; if so, they are considered to have made their best film early in their career. The second criterion was that the first half of their career must be better than the second half;


**Why**

Regarding the data collected for each actor, we chose to include only their films with films with at least 100 votes to prevent a film with very few ratings—and therefore very little recognition—from meeting the criteria for, say, a film’s rating at the end of their career.
Next, regarding our decision to establish these two criteria to avoid several issues: First, we considered the beginning of an actor’s career as the first third to determine whether their highest-rated film was released early on. However, we felt that this check alone was insufficient, so we examined both halves of their careers to see whether this best film stood alone or not (i.e., a single major film surrounded by weaker ones). We also considered that an actor who was more popular at the start of their career should have a first half with higher-rated films than the second half of their career.

**Alternatives and trade-offs**

The main trade-off in this approach is that we divided an actor’s career into halves based on the number of years, not the number of films. In fact, an actor might very well have made 20 films in 10 years and then only 5 films in 20 years, meaning that the second half of their career, as we define it, represents a very small sample of films, which could skew the results. An alternative would be to divide their career based on the number of films to avoid this.

#### Multi-talented professionals (q8)

**Method**

For this question, a person was considered multi-talented if they had at least 3 films in each of at least 2 different roles.
To identify this, we listed the possible roles, such as actors, directors, or screenwriters.
Once this list was compiled, we counted, for each person, their films in each role. A person is considered multi-talented if they have at least 3 films in at least 2 roles.
Next, for each film by a multi-talented person, we checked whether the person had only one role or multiple roles within each film. We then compare the average ratings of the two groups of films.
To facilitate further comparisons, we generated three main outputs: the first showing the average rating of films in which the person plays multiple roles, the second comparing each person to themselves, and the third listing the top multi-talented individuals.

**Why**

First, we compiled the various roles from different tables to ensure we didn’t miss any, and we removed duplicates once the compilation was complete to avoid counting the same person twice for the same role in the same film.
We chose to classify a professional as “multi-talented” only if they had at least three films in each of at least two roles, since directing a single film does not necessarily make someone a director—they need to have multiple experiences.
We decided to compare the person’s own accounts of their multi-role experiences with their single-role experiences to gain a better understanding of whether taking on multiple roles, rather than focusing on just one, has an impact on their performance.

**Alternatives and Trade-offs**

The threshold of three films to be considered multi-talented isn’t necessarily the best; if we raised this threshold, we’d end up with only professionals in each specific role.
An alternative to our method could be to compare multi-role performers with one another—that is, to compare actors-directors with each other rather than simply comparing multiple roles against a single role.


## 3. Data enrichment

As a reminder, in this section, for each movie released between 1980 and 2025, we were tasked with enriching the dataset by including a synopsis for the top 10 movies by genre for each year and biographical information for at least three people for each movie.

### Results

Here are the results obtained after running the enrichment process:

#### Coverage

| Dataset | Rows | With Wikidata | With Wikipedia | With text | Coverage |
| ------- | ---: | ------------: | -------------: | --------: | -------: |
| Films | 7,182 | 6,681 | 6,364 | 5,916 | 82.4% |
| Principals | 21,500 | 19,954 | 19,221 | 19,217 | 89.4% |

#### Biographies per film

| Biographies | Films |
| ----------: | ----: |
| 0 | 371 |
| 1 | 376 |
| 2 | 443 |
| 3 | 5,985 |

7 of the 7,182 selected films have no credit in the data, so they do not appear in this table.

#### Reasons for missing data

| Films: reason | Count |
| ------------- | ----: |
| No Wikidata item with this IMDb id | 501 |
| No plot/synopsis section in the article | 445 |
| No English Wikipedia article linked in Wikidata | 317 |
| Synopsis too short or empty | 3 |

| Principals: reason | Count |
| ------------------ | ----: |
| No Wikidata item with this IMDb id | 1,546 |
| No English Wikipedia article linked in Wikidata | 733 |
| Biography too short or empty | 3 |
| Empty lead section | 1 |

#### Examples

Here is an example of an enriched row from the CSV file `films_sample.csv` (which provides a preview of the final enriched movie table): 
``` 
tt14205554,KPop Demon Hunters,2025,"Long ago, demons preyed on humans, feeding drained souls to their ruler, Gwi-Ma. In time, three women became demon hunters and used their singing voices to create an effective though incomplete magica",https://en.wikipedia.org/w/index.php?oldid=1382150513
```

The example is based on the movie *KPop Demon Hunters*. The row recorded in the CSV file contains the fields `tconst`, which is the movie’s IMDb ID; `primaryTitle`, which is the movie’s title; `startYear`, which is its release year; `synopsis_start`, which is the enriched data and contains a description of the beginning of the movie; and `source_permalink`, which corresponds to the source used for the enrichment.

Another example from `principals_sample.csv` (which provides a preview of the final, enriched `principals` table):
```
tt0080487,Bill Murray,actor,1950-09-21,Wilmette,"William James Murray (born September 21, 1950) is an American actor and comedian, known for his deadpan delivery in roles ranging from studio comedies to independent dramas. He has received several ac",https://en.wikipedia.org/w/index.php?oldid=1381610814
```

The example is based on the actor Bill Murray and contains the fields `tconst` for their IMDb movie ID, `primaryName` for their first and last names, `category` indicating whether they are actors, directors..., `birth_date` for their date of birth, `birth_place` for their place of birth, `biography_start` for an excerpt from their biography, and `source_permalink` for the source of the information.

#### Key takeaways

After the enrichment process was completed, most of the data became available, with 82.4% of films and 89.4% of people successfully enriched. The goal of obtaining 3 biographies per film was largely achieved, as 5,985 out of 7,175 films with credits have 3 biographies. The remaining 1,190 films have fewer than 3 biographies, including 371 with none.

The main source of missing data, for both films and people, is the absence of a Wikidata item associated with the IMDb identifier (501 films and 1,546 people). Another significant cause is the absence of a synopsis section in the Wikipedia article (445 films).

This is because, for a person, we use the introduction of the Wikipedia article, which almost always exists and provides the information needed for enrichment.

For a movie, enrichment is more complicated because we look for specific elements in a specific section, not just at the beginning of the page. Specifically, the code retrieves the entire article and searches for a section containing “Plot,” “Synopsis,” “Plot summary,” “Premise,” or “Story.”

### Methodological explanation

#### Selection of films and principals

**Method**

To select the movies, we start with feature_movies, which we’ve already used in the exploration and which corresponds to IMDb titles of type movie that are longer than 40 minutes and have a rating. We then apply a filter on the release year, which must be between 1980 and 2025. We then break the movies down by genre, keeping in mind that in the IMDb data, a movie can have up to 3 genres.

Next, we rank the movies using `partitionBy("startYear", "genre")` for each year-genre pair, sorted by `numVotes` (the number of votes) in descending order, and keep the top 10. In the event of a tie, we break the tie based on their `tconst` identifier; this ensures that the ranking remains consistent from one run to the next (deterministic).

We noticed that a movie could appear in the top 10 of multiple genres in the same year, so we decided to include it only once using `groupBy("tconst")`. We merge all rows for the same movie into a single row, then combine the genres, sort them, and separate them with `|`.

To retrieve the people, in `title_principals`, IMDb provides a list of numbered credits for each movie (`ordering` = order in the credits: 1, 2, 3…) that correspond to the “principals.” For IMDb, these include actors, as well as directors, screenwriters, composers, etc.

We also apply a filter to keep only the first 5 using `select_targets`; these will be our “candidates.” After this filter, we obtain 35,780 lines of candidates, representing 18,696 distinct individuals (a person may appear in multiple films, but we count them only once).

Next, using `build_enriched`, we sort the candidates for each film by those who have a biography, then by order of appearance in the credits, and keep only the top 3 (this gives us 21,500 rows).

**Why**

Here’s why we designed it this way. The `numVotes` metric measures the attention a film receives. The problem with ratings is that a film rated 9.5 by 12 people would rank higher in the top list than a film that is better known and has 2 million votes averaging 8.0. We redefined our criteria to determine how we would rank films. In our view, this allows us to create a popularity-based top list, since it takes a film’s fame into greater consideration and views a film’s popularity as being measured by the number of votes.

For the second guideline, we aimed to obtain at least 3 biographies per film by initially considering the top 3 credited people. However, some of them did not have an available Wikipedia biography, making it difficult to reach this goal. To improve the results, we expanded the selection to 5 candidates per film, prioritizing the most important credited people. This way, we increased the number of films with 3 biographies from 5,298 to 5,985, demonstrating its effectiveness.

**Alternatives and trade-offs**

With these choices, we have 14,280 additional lines of candidates and 7,775 new people to search for, which means more queries and a longer data collection process.

This also means that we no longer necessarily keep the first three people listed in the credits; the trade-off is that we’ve chosen to include a biography rather than a “headliner.” Despite this choice, 1,190 films with credits still have fewer than three biographies; the goal has not been fully achieved everywhere—371 have none, and 7 films have no IMDb credits.

The “top by `numVotes`” ranking favors recent and popular films—which are heavily featured on Wikipedia—that have a large number of ratings regardless of the rating’s value: this could potentially has an impact on well-rated but lesser-known films. The popularity ranking is therefore measured solely by the number of people who have rated the film.

A fairer alternative would have been to rank by weighted rating in order to take into account not only the number of ratings—and thus popularity—but also the quality of the ratings (depending on how one defines “top”).

As for the number of people, we could have limited it to just the first three credits—that would have simplified the data enrichment process—but we would have ended up with fewer films featuring three biographies. We could also have included more than five candidates, but that would have required more people to look up and more requests. Finally, we could have added a second source for the missing people.


#### Data source and matching

**Method**

For the source and mapping, it works in two steps.

The first step involves Wikidata (a structured database linked to Wikipedia); each Wikidata item can have the IMDb identifier in the P345 property. A script sends a SPARQL query (Wikidata’s query language) in batches of 100 identifiers: “Give me the items where P345 is one of these 100 identifiers, and their English Wikipedia article, if it exists.”

For people, the same query also retrieves structured facts including date of birth (P569), date of death (P570), place of birth (P19), and nationality (P27) in English. The results are written to `wikidata_films.jsonl` and `wikidata_people.jsonl`.

The second step involves Wikipedia using the MediaWiki API. For each article found, the script retrieves the article’s plain text.
For a person, it retrieves only the introduction (the paragraphs before the first heading), in batches of 20 articles per request. The introduction summarizes the person’s life and career: “Lee Marvin (1924–1987) was an American actor…”.

For movies, the introduction does not recount the story (“X is a 2010 film directed by Y…”). The script therefore requests the full article—one film per query—and then searches for the first section whose title is exactly `Plot`, `Synopsis`, `Plot summary`, `Premise`, or `Story`. It follows redirects, so if the Wikidata link points to an old title, Wikipedia returns the correct article.

**Why**

It’s designed this way because a title can be ambiguous. For example, several movies are called *Heat*, remakes bear the same title as the original, and the “Heat” page is about physics. An identifier refers to a single work or a single person, which prevents text from another movie or a namesake from being pasted in. The match is exact, not guessed.

Furthermore, Wikidata is the natural bridge between IMDb and Wikipedia; it stores the links to both, populated by contributors. Wikipedia is written and reviewed by humans under an open license; the text is reusable if the source is cited (CC BY-SA 4.0). Wikidata is under CC0 (public domain).

Finally, the guideline allows for a single source if the omissions are noted, which is what we’ve done.

**Alternatives and trade-offs**

One trade-off is that without a link in Wikidata, nothing is found even if the article exists (501 films, 1,546 people included). Another trade-off is that only English is covered (317 films and 733 people included without an English article). It is primarily non-English-language films and people that are missing, which could potentially create a bias.

Another trade-off is that an article without a recognized section does not provide a synopsis (445 films). If we used a longer list of titles, we would retrieve more synopses, but this would risk including a section that is not a summary (e.g., “Production”).

As for other alternatives, we could have searched by title and year; we would have gotten more results but would have encountered errors due to homonyms. We could also have used articles from other languages; we would have had broader coverage, but it would have taken longer because we would have had to translate the texts, and the mix of languages would have caused other problems. We could also have relied on another open database while adhering to its license terms and rules.

#### Rate limits and scraping policy

**Method**

For rate limits and scraping policy, we use a `PoliteClient` class. We make one request at a time and never in parallel. We have also introduced a minimum delay between two requests to the same service (3 seconds for Wikidata, 0.5 seconds for Wikipedia).
We also use a descriptive `User-Agent` that allows us to track the origin of each request; the User-Agent identifies the program making the request so that administrators can identify and contact the author.

We have also implemented several rules to prevent latency issues. If the servers are more than 5 seconds behind, Wikipedia rejects the request and the script waits before retrying. If too many requests are sent, the script waits.
If the server sends a `Retry-After` header, the script waits exactly that amount of time; otherwise, it waits for progressively longer periods (2, 4, 8, 16 seconds, etc.). After 6 failed attempts, it stops. We apply the same principle of progressively longer wait times in the event of a network outage.

To reduce the number of requests, we group 100 identifiers per Wikidata request and 20 biographies per Wikipedia request. For movie synopses, we send them one by one. Finally, each retrieved result is added to the JSONL file immediately after retrieval. To avoid re-reading articles that have already been read, when the script resumes and restarts, it reads the IDs that are already present and skips them.

**Why**

Since these services are free and shared by many users, it is important to respect their usage limits. The script must therefore slow down when the server requests it to avoid overloading the server. The rules we have established comply with the sites’ limits and guidelines (API:Etiquette, Wikimedia User-Agent policy).

For `maxlag`, the script is put on hold when Wikipedia’s servers are delayed; human readers take priority over automated scripts. Resuming the process prevents having to start over after an outage: the last run only retrieved the 7,775 new people, not the 10,921 already found.

**Alternatives and trade-offs**

One trade-off is that the data collection process is slow—about 57 minutes for the 6,364 movie articles and about 1 hour and 30 minutes for the entire collection. Movies are more resource-intensive because a separate request is required for each movie.

#### Provenance and missing data

**Method**

To ensure traceability and handle missing data, each row in the final tables includes several pieces of information: the link to the Wikipedia article (`source_url`), the link to the exact version viewed (`source_permalink`), constructed from the revision number provided by the API, the Wikidata item associated with IMDb (`wikidata_url`), and the date and time of collection in UTC. The `source_license` field indicates the CC BY-SA 4.0 license when an article has been found. Finally, the `note` field specifies the reason for any missing data, while retaining the row with an empty text field.

The `note` field may indicate that no Wikidata item is linked to the IMDb identifier, that no English Wikipedia article is linked to the Wikidata item, that the article was found but no plot or synopsis section was identified, that the introduction is empty, or that the text is too short after cleaning.

**Why**

The guidelines require that the source of each record be retained to ensure data traceability. Since Wikipedia articles can change over time, the `source_permalink` field allows you to retrieve the exact version used via the `oldid` parameter in the link, even if the article is subsequently modified. Unlike a standard link, which points to the current version, this link preserves access to the version that was viewed. This also ensures compliance with the CC BY-SA 4.0 license, which requires, among other things, that the source be cited when the text is reused.

We have chosen not to remove lines with no text because doing so would hide gaps and distort the coverage.
To comply with the traceability guideline “notes should be programmatically added to the output when information is not currently available” the system we developed directly indicates at which stage data is lost and, therefore, what needs to be improved.

**Alternatives and trade-offs**

The trade-off is that this adds extra columns to each table, as well as rows with no text that subsequent uses must filter out. Another limitation is that for the 7 movies without IMDb credits, they have no enriched rows and therefore no `note` on the people side either.

An alternative would have been to completely eliminate and remove the films without credits, but that would have impacted the dataset and its accuracy.

#### Cleaning and schema enforcement with Spark

**Method**

Data cleaning is performed using Apache Spark, while data collection is carried out separately. Spark loads the JSONL files using an explicit schema and FAILFAST mode to detect format errors.

It then removes duplicates based on the `imdb_id` and cleans the text using regular expressions to eliminate section titles, empty parentheses, and unnecessary spaces. Text strings shorter than 50 characters are set to null, while the corresponding rows are preserved and an explanation is provided in the note field. Wikidata dates are also validated to retain only those in the YYYY-MM-DD format.

Finally, the cleaned data is merged with the IMDb information (name_basics) and then saved in Parquet format in data/gold/. These files are then processed again to generate the CSV files containing the results.

**Why**

This technique separates data collection from data cleaning to prevent Spark from sending multiple API requests in parallel, which would make it difficult to control the delays between requests. Using an explicit schema ensures consistent data types, while FAILFAST mode allows for rapid detection of format errors. Removing duplicates prevents repetitions caused by resuming the collection process, and cleaning the text corrects imperfections in the Wikipedia export. Finally, the Parquet format preserves data types and allows for the rapid generation of CSV files without having to redo the joins.


**Alternatives and trade-offs**

However, this approach has certain limitations. FAILFAST mode stops processing as soon as a line is malformed, while the 50-character threshold may exclude short but valid texts. Furthermore, regular expressions do not necessarily cover all possible cases. An alternative would be to use a permissive mode with error tracking or to adjust the text length threshold.

#### Scalability

**What already scales**

The project is designed to handle larger volumes of data. The parameters (--top-n, --candidates, --year-min, --year-max) allow you to expand the scope to include all movies without modifying the code.

Spark writes the data directly without loading it entirely into the driver memory by removing toPandas(). Optimized handling of done_ids and the resumption system facilitate the collection of large volumes of data.

Finally, data cleaning and joins using Spark allow processing to be distributed across multiple cores or machines.

**Limits**

The main limitation of the project stems from the APIs, whose response times slow down the data collection process. With a minimum of 0.5 seconds per article, collecting data on 300,000 films would take approximately 42 hours, not counting principals and Wikidata. This duration is only an estimate. Furthermore, the use of coalesce(1) limits CSV writing to a single task, which can be problematic for large volumes.

An alternative would be to use Wikimedia dumps, which allow data to be processed directly with Spark, without API requests. However, these files are large, and their data is not always up to date.

## 4. Streaming data processing

### Results

During the final run on 8 November 2026, the pipeline matched 157 Wikipedia change events to 116 distinct IMDb entities over a period of 18 minutes and 58 seconds. The average time between the Wikipedia event timestamp and its arrival in Kafka was 3.5 seconds.

| Entity type | Human edits | Bot edits | Total |
| ----------- | -----------: | --------: | ----: |
| People | 75 | 3 | 78 |
| Titles | 76 | 3 | 79 |
| **Total** | **151** | **6** | **157** |

The five-minute tumbling-window query wrote six entity-type aggregates, covering three completed time intervals:

| Event-time window (UTC) | People | Titles | Total |
| ---------------------- | ------: | -----: | ----: |
| 15:20–15:25 | 5 | 10 | 15 |
| 15:25–15:30 | 32 | 21 | 53 |
| 15:30–15:35 | 22 | 25 | 47 |
| **Total in written windows** | **59** | **56** | **115** |


### Late arriving data and first impression

Wa  had to make a decision regarding late arriving data. The stream uses event time—the timestamp supplied by Wikimedia rather than the time Spark happens to process a message. We set a two-minute watermark on `event_time` so Spark can handle the late events via windowed queries. Once the watermark has passed the end of a window, Spark writes that window's final aggregate and can discard its state. Events that arrive after their window has been finalized are excluded from those window counts and are collected by Spark's "watermark drop" metric.

The two-minute delay was chosen to leave room for ordinary delivery delays without keeping window state open for too long. We considered it a good compromise. The measured average event-to-Kafka delay in the final run was 3.5 seconds, well below that allowance. The counterpart is that events delayed by more than the watermark can no longer update a closed window. The job also stopped before all event-time windows had closed: 115 of the 157 matches appear in the completed five-minute aggregates, while the remaining 42 remain available in the event-level output but are not included in a written window. If we had chosen a longer watermark, we could retain more late data, but it would delay the final output and retain more state (it could also leave more end-of-run windows unfinished).

### Methodological explanation

#### Results window

We chose a ten-minute sliding-window results for the pipeline, advancing every five minutes. Since these windows overlap, an event can contribute to more than one sliding-window aggregate as seen in class. The event-level matches, the window aggregates and the progress metrics were all written separately, so the detailed matches can be inspected independently of the summaries.

#### Event source and Kafka ingestion

Our producer listens to Wikimedia’s live recentchange feed and sends selected events to the Kafka topic wikistreams. It only keeps edits and new pages from article pages on the English, French, German, Spanish and Italian Wikipedias. We chose these filters to keep the amount of data manageable while still including several languages, which gives us a better chance of finding IMDb-related changes during a short run. Messages are keyed by page title, and if the connection drops the producer reconnects from the last event it received.

#### Matching Wikipedia pages to IMDb

Spark reads the Kafka messages as JSON and uses a defined schema to interpret their fields. It also creates a reference list from IMDb, including movies and series with at least 1,000 votes, plus the actors, actresses and directors credited on them. To compare IMDb names with Wikipedia page titles, Spark converts both to lowercase, removes extra spaces and drops any qualifier at the end, such as “(1995 film)” before joining the incoming events to the reference list using these normalized names.

We used the 1,000-vote threshold to focus on more popular titles and keep the reference list manageable. If several IMDb entries have the same normalized name, the pipeline selects the one with the most votes and records how many candidates shared that name. It also checks qualifiers to avoid matching, for example, a song page to a film title. When a title has no qualifier, it must contain at least two words to be considered a match. These checks reduce false matches, but they can also exclude a valid one-word title or fail to resolve every namesake. Wikidata could provide more exact matches through identifiers, but looking up each event would require extra API requests. Fuzzy matching might find more pages, but could also introduce more incorrect matches.

#### Windowed outputs and trade-offs

We chose to run three streaming queries, triggered every 30 seconds. The first saves each matched event, including when it happened and when it reached Kafka. The other two summarize events by entity type: one uses fixed five-minute windows, while the other uses ten-minute windows that move forward every five minutes. For each window, we count events and distinct entities, separate human edits from bot edits, and calculate the net change in page size.

We write the results to Parquet and use a separate checkpoint for each query, so the stream can resume if it is interrupted. To start a completely fresh run, we need to remove the old checkpoints and reset the Kafka topic.

We also chose the append mode for the window queries, which means Spark writes each window once it considers it complete. This keeps the output simple, but windows that have not closed by the end of the run will not appear in the summaries. Finally, we included sliding windows to show recent activity over a longer period.

#### Handling late events

As explained earlier, our two-minute watermark gives slightly delayed events time to arrive, while still allowing Spark to close windows and clear their state. A longer watermark might include more late events in the window totals, but it would also delay those results and keep the state open longer. We also chose to use event time rather than processing time, so the windows reflect when the Wikipedia changes happened, not when Spark received them. The detailed matches are saved separately, so events that arrive too late for a window can still be found there.

## 5. Open question

### Question and motivation

The question we’ve chosen for this section is: Do director-actor duos who frequently work together receive higher ratings?

In the world of cinema, there are always directors who are better known for their work with a particular actor than for the films they’ve made without him. We thought of duos like Martin Scorsese and Robert De Niro, who are very well known together but somewhat less so when they’re not working together.
So we wondered whether working together actually improves the quality of the films because of the mutual understanding that develops between the duo, or whether it’s simply a matter of teaming up with the same person again because it worked well the first time.

To do this, we didn’t use any external data—only IMDb data from the following tables:
title_basics for the film genre and year, title_ratings for ratings and votes, title_directors for directors, title_principals to identify which actors appeared in which films, and finally the name_basics table for the names of the individuals (directors and actors).


### Results and key takeaways

| Collaboration | Films | Duos | Avg rating | Rating vs director's mean | Avg votes |
| ------------- | ----: | ---: | ---------: | ------------------------: | --------: |
| 1st | 57,097 | 57,097 | 6.377 | +0.004 | 42,954 |
| 2nd | 7,225 | 7,225 | 6.474 | +0.004 | 47,372 |
| 3rd | 2,349 | 2,349 | 6.470 | −0.043 | 50,266 |
| 4th | 1,029 | 1,029 | 6.481 | −0.042 | 50,701 |
| 5th and more | 1,452 | 540 | 6.466 | −0.073 | 36,139 |

| Duos with ≥ 2 films | Later films − first film (avg) | Duos improving |
| ------------------: | -----------------------------: | -------------: |
| 7,225 | −0.267 | 27.1% |

| Genre | Occasional duos: films | Rating vs director's mean | Recurring duos: films | Rating vs director's mean |
| ----- | ---------------------: | ------------------------: | --------------------: | ------------------------: |
| Action | 12,183 | −0.116 | 2,592 | +0.078 |
| Comedy | 20,521 | −0.085 | 4,166 | +0.044 |
| Crime | 11,441 | +0.079 | 1,880 | +0.174 |
| Drama | 36,678 | +0.069 | 4,950 | +0.158 |
| Horror | 7,006 | −0.132 | 701 | +0.041 |
| Sci-Fi | 2,950 | −0.249 | 433 | −0.028 |
| Thriller | 8,489 | −0.053 | 942 | +0.119 |

| Director | Actor | Films together | Avg rating | Vs director's mean | Years |
| -------- | ----- | -------------: | ---------: | -----------------: | ----- |
| Kartal Tibet | Kemal Sunal | 29 | 6.96 | +0.43 | 1976–1990 |
| Gerald Thomas | Kenneth Williams | 23 | 6.11 | +0.13 | 1958–1978 |
| Gerald Thomas | Sidney James | 21 | 6.27 | +0.28 | 1960–1974 |
| Akira Kurosawa | Toshirō Mifune | 18 | 7.84 | +0.30 | 1948–1965 |
| Priyadarshan | Mohanlal | 18 | 7.48 | +0.55 | 1985–2021 |
| John Ford | John Wayne | 14 | 7.26 | +0.26 | 1939–1963 |

Based on these results, we can make a few observations:

The first is that the raw score rises slightly with collaborations, but when compared to the director’s average, the difference is virtually zero. This is mainly due to the fact that good directors often work with the same actors, which explains the slight increase in the score. Once this effect is accounted for, the difference is virtually zero.

The second observation is that, within the same director-actor pairing, subsequent films are not as good as their first collaboration. On average, there is a difference of -0.27 points between their first film and the others, and only 27% of these pairings manage to improve.

The final observation is that recurring duos generally perform better than occasional duos when compared to the director’s average rating for other films.

We can therefore draw a certain conclusion from these observations: the first film by a duo that has stood the test of time is often a very good film.
We can then hypothesize that the director recasts the actor with whom the film was a success, but the other films do not fare as well. However, if it works out again, the duo will often become a recurring one, built on the success of several films rather than just one—which explains why recurring duos perform better than occasional ones.


### Methodological explanation

**Method**

To try to answer the question as best as possible, we first compiled a list of all movies with at least 1,000 votes, along with their release years.
We then identified each pairing consisting of the director and one of the movie’s lead actors; each pairing is assigned a collaboration number, which allows us to list them in chronological order.
We can then calculate a relative rating—that is, a rating for the film in which the pair in question appears, compared to the director’s typical ratings—to see the difference between the two.
Finally, we excluded directors who made fewer than 5 films and grouped the pairs into two categories: recurring pairs (3 or more films together) and occasional pairs (1 or 2 films together).

In the end, four CSV files are generated: the first shows the average rating by collaboration number; the second shows the difference between recurring and occasional duos, by genre; the third compares, for each duo, the first collaboration with subsequent ones; and the last lists the 30 duos with the most films together.


**Why**

We included only movies with at least 1,000 votes to ensure reliable ratings, and movies well-known enough for the pairings to make sense.
We also limited our analysis to lead actors (the first three in the credits) to ensure that pairings with directors were meaningful and to avoid creating numerous pairings with supporting actors, which would not be particularly relevant to our research question.
We also chose to compare the films featuring the actor-director pair with the director’s other films to highlight the differences from that director’s other works.

**Alternatives and trade-offs**

However, we observed certain trade-offs. The first is that the director’s average counts some films multiple times; since we are considering director-lead actor pairings, a film with three lead actors will count three times in this average—once per pairing.
We could also have measured success differently, such as by the number of votes, or using external data like box office receipts or awards.

