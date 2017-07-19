#!/usr/bin/env python3
import psycopg2
import datetime

conn = psycopg2.connect(database="news")
cursor = conn.cursor()

cursor.execute("""SELECT articles.title, count(log.path) AS views
                FROM articles, log
                WHERE log.path = concat('/article/', articles.slug)
                GROUP BY articles.title
                ORDER BY views DESC LIMIT 3;""")
question_one_results = cursor.fetchall()

cursor.execute("""SELECT authors.name, count(log.path) AS views
                FROM authors, log, articles
                WHERE log.path = concat('/article/', articles.slug)
                AND articles.author = authors.id
                GROUP BY authors.name
                ORDER BY views DESC;""")
question_two_results = cursor.fetchall()

cursor.execute("""SELECT date(time) AS dates, cast(count(*) AS float) /
                (SELECT cast(count(*) AS float)
                FROM log WHERE status = '404 NOT FOUND') AS errorPerc
                FROM log GROUP BY dates
                ORDER BY errorPerc;""")
question_three_results = cursor.fetchall()

output = open("export.txt", "w")
with output as f:
    f.write("Question 1"+"\n")
    for row in question_one_results:
        f.write("%s -- " % str(row[0]))
        f.write("%s views\n" % str(row[1]))

    f.write("\n"+"Question 2"+"\n")
    for row in question_two_results:
        f.write("%s -- " % str(row[0]))
        f.write("%s views\n" % str(row[1]))

    f.write("\n"+"Question 3"+"\n")
    for row in question_three_results:
        if isinstance(row[0], datetime.date):
            format_date = row[0].strftime('%b %d, %Y')
            f.write("%s" % format_date)
        f.write(" -- %s%% error\n" % str(row[1]))
output.close()
conn.close()
