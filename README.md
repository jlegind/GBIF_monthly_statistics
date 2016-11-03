# GBIF_monthly_statistics
One-click (almost) generation of monthly stats for GBIF indexing and user downloads

### Monthly stats workflow
$ cd GBIF_monthly_stats<br>
$ python monthly_stats.py “startdate” “enddate”<br>
$ cd Documents<br>
$ cp final.csv /home/jan/MonthlyStatsWeb/stats/data<br>
$ git add data/<br>
$ git commit –m “stats month thecurrentyear”<br>
$ git push origin master<br><br>
#### UPDATE for new python settings:<br>
$ sudo python2.7 monthly_stats.py “startdate” “enddate”
