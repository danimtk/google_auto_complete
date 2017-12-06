# Google Auto Complete

Using MapReduce to implement Google Auto-Completion based on N-Gram Language Model.
<div align="center">
  <img src="https://user-images.githubusercontent.com/13844740/33495739-90d08e48-d702-11e7-8309-74bd821b5110.gif" height="284px">
</div>

## Description
- Step 1: Build <b>N-Gram</b> library over Wiki dataset, then build <b>Language Model</b> based on <b>statistical probability distribution</b>, generate structured data and store it in database.
- step 2: Using <b>PHP/Ajax</b> and <b>jQuery</b> to retrieve data from database to implement <b>real-time</b> auto-completion. Then display the auto-complete features of <b>search engine</b> on web page just like Google does.

## WorkFlow
Wiki dataset (or any other corpus) --> <br/>N-Gram Library (Hadoop/MapReduce) --> <br/>Language Model (Hadoop/MapReduce) --> <br/>Database (MySQL) --> <br/>Web Interface (PHP/Ajax and jQuery)

## My Running Environment
- <b>Docker</b>
- <b>Hadoop</b>
- <b>MySQL</b>
- <b>Apache</b>

## Citation
```
  @misc{ye2017googleautocomplete,
    author = {Wengao Ye},
    title = {Google Auto Complete},
    year = {2017},
    publisher = {GitHub},
    journal = {GitHub repository},
    howpublished = {\url{https://github.com/elleryqueenhomels/google_auto_complete}}
  }
```
