import psycopg2
import datetime

conn = psycopg2.connect(database="news")
cursor = conn.cursor()

cursor.execute("""select articles.title, count(log.path) as views
                from articles, log
                where log.path = concat('/article/', articles.slug)
                group by articles.title
                order by views desc limit 3;""")
question_one_results = cursor.fetchall()

cursor.execute("""select authors.name, count(log.path) as views
                from authors, log, articles
                where log.path = concat('/article/', articles.slug)
                and articles.author = authors.id
                group by authors.name
                order by views desc;""")
question_two_results = cursor.fetchall()

cursor.execute("""select date(time) as dates, cast(count(*) as float) /
                (select cast(count(*) as float)
                from log where status = '404 NOT FOUND') as errorPerc
                from log group by dates
                order by errorPerc;""")
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
