# ECE Big Data Processing   IMDb project report

## 1. Data preparation

### Results

### Methodological explanation

## 2. Data exploration

### Results

### Methodological explanation

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

For the second guideline, we aimed to obtain at least 3 biographies per film by initially considering the top 3 credited people. However, some of them did not have an available Wikipedia biography, making it difficult to reach this goal. To improve the results, we expanded the selection to 5 candidates per film, prioritizing the most important credited people. This approach increased the number of films with 3 biographies from 5,298 to 5,985, demonstrating its effectiveness.

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

This approach separates data collection from data cleaning to prevent Spark from sending multiple API requests in parallel, which would make it difficult to control the delays between requests. Using an explicit schema ensures consistent data types, while FAILFAST mode allows for rapid detection of format errors. Removing duplicates prevents repetitions caused by resuming the collection process, and cleaning the text corrects imperfections in the Wikipedia export. Finally, the Parquet format preserves data types and allows for the rapid generation of CSV files without having to redo the joins.


**Alternatives and trade-offs**

This approach has certain limitations. FAILFAST mode stops processing as soon as a line is malformed, while the 50-character threshold may exclude short but valid texts. Furthermore, regular expressions do not necessarily cover all possible cases. An alternative would be to use a permissive mode with error tracking or to adjust the text length threshold.

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

### Late arriving data

### Methodological explanation

## 5. Open question

### Question and motivation

### Results and key takeaways

### Methodological explanation
