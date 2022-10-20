====== Lukáš Líška (lukas.liska) - Private Space ======

**Téma projektu**: O21 - Vytvorenie jednoduchej služby, ktorá po zadaní mena filmového kritika, užívateľovi zobrazí štatistiku kritikových reviews (aj s filmami, ktoré reviewoval) s jednoduchým pomerom reliability a inými špecifickými informáciami (Python, Framework?).

**Dáta**: https://www.kaggle.com/datasets/ebiswas/imdb-review-dataset?resource=download (JSON format)
Dáta obsahujú iba reviews (cez 5 miliónov záznamov).

V danom datasete analyzujem reviews používateľov a hodnotím kritikovu reliability a aktivitu (či jeho review pomohol ľuďom alebo naopak, odkedy reviewuje filmy, či do reviews píše aj spoilery, či bol review napísaný tesne po vydaní filmu alebo či o dosť neskôr, a iné dodatočné info ako napr. priemerný rating). Ako bonus mám tiež implementovaný full text search, kde vie user zadať nejaké slová a vráti mu to top 10 reviews, ktoré dané slová obsahujú (zoradené podľa najlepšej zhody).

**Sample dát**:
<code>{"review_id": "rw1133942", "reviewer": "OriginalMovieBuff21", "movie": "Kill Bill: Vol. 2 (2004)", "rating": "8", "review_summary": "Good follow up that answers all the questions", "review_date": "24 July 2005", "spoiler_tag": 0, "review_detail": "After seeing Tarantino's Kill Bill Vol: 1, I got to watch Vol. 2 the next day after seeing it. I liked the first one but didn't really know everything that was going on, but just knowing that she set off the kill 5 individuals who left her dead at her wedding. So I saw Kill Bill: Vol 2 and liked it highly. The movie answered all of my questions to the previous one and had much of a better story and was not unrealistic that much. We finally get to see Bill, who is played by David Carradine who had a really good role in the movie. There is a great conclusion to the movie and had a very good story, along with likable characters, my favorite being Budd. Overall, good movie that answers tons of questions. I recommend Kill Bill Vol. 2.Hedeen's Outlook: 8/10 *** B", "helpful": ["0", "1"]}</code> 

**Typy indexov**:
V projekte mám spravené 2 typy indexov: Reviewer index, čo je field-based index na field "reviewer" a TF-IDF index čo je full-text index s Euklidovskou normalizáciou.

**Typy search queries**:
Reviewer index: <code>reviewer:"swanson"</code> (query vráti všetkých reviewerov, ktorý začínajú na swanson - case insensitive)
TF-IDF index: tu mám spravené 2 typy dopytov: 
1. Napíšeme slová (<code>tarantino kill bill</code>) a query nám vráti top 10 reviews, kde sa tieto slová nachádzajú s braním do úvahy ich blízkosť a počet v dokumente.
2. Napíšeme slová do úvodzoviek aka frázu (<code>"jessica alba actress"</code>) - query nám vráti top 10 reviews kde sa dané slová nachádzajú v presnom poradí za sebou (samozrejme bez brania do úvahy Stopslov, ktoré sa prečistia ešte pred vyhľadávaním)
