
# coding: utf-8

# # Hadoop Streaming assignment 1: Words Rating

# The purpose of this task is to create your own WordCount program for Wikipedia dump processing and learn basic concepts of the MapReduce.
# 
# In this task you have to find the 7th word by popularity and its quantity in the reverse order (most popular first) in Wikipedia data (`/data/wiki/en_articles_part`).
# 
# There are several points for this task:
# 
# 1) As an output, you have to get the 7th word and its quantity separated by a tab character.
# 
# 2) You must use the second job to obtain a totally ordered result.
# 
# 3) Do not forget to redirect all trash and output to /dev/null.
# 
# Here you can find the draft of the task main steps. You can use other methods for solution obtaining.

# ## Step 1. Create mapper and reducer.
# 
# <b>Hint:</b>  Demo task contains almost all the necessary pieces to complete this assignment. You may use the demo to implement the first MapReduce Job.

# In[1]:


get_ipython().run_cell_magic('writefile', 'mapper1.py', '\n# Your code for mapper here.\nimport sys\nimport re\n\nreload(sys)\nsys.setdefaultencoding(\'utf-8\') # required to convert to unicode\n\nfor line in sys.stdin:\n    try:\n        article_id, text = unicode(line.strip()).split(\'\\t\', 1)\n    except ValueError as e:\n        continue\n    words = re.split("\\W*\\s+\\W*", text, flags=re.UNICODE)\n    for word in words:\n        print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % 1\n        print "%s\\t%d" % (word.lower(), 1)')


# In[2]:


get_ipython().run_cell_magic('writefile', 'reducer1.py', '\nimport sys\n\ncurrent_key = None\nword_sum = 0\n\n# Your code for reducer here.\nfor line in sys.stdin:\n    try:\n        key, count = line.strip().split(\'\\t\', 1)\n        count = int(count)\n    except ValueError as e:\n        continue\n    if current_key != key:\n        if current_key:\n            print "%s\\t%d" % (current_key, word_sum)\n        word_sum = 0\n        current_key = key\n    word_sum += count\n\nif current_key:\n    print "%s\\t%d" % (current_key, word_sum)')


# In[3]:


get_ipython().run_cell_magic('writefile', 'debug_data.txt', '1\ta a b c\n2\tb b a c')


# ## Step 2. Create sort job.
# 
# <b>Hint:</b> You may use MapReduce comparator to solve this step. Make sure that the keys are sorted in ascending order.

# In[4]:


get_ipython().run_cell_magic('writefile', 'reverse_mapper1.py', '\nimport sys\n\nfor line in sys.stdin:\n    try:\n        word, count = line.strip().split(\'\\t\', 1)\n        count = int(count)\n        print("{0}\\t{1}".format(count, word))\n    except ValueError as e:\n        continue')


# In[5]:


get_ipython().run_cell_magic('writefile', 'reverse_reducer1.py', '\nimport sys\n\nfor line in sys.stdin:\n    try:\n        count, word = line.strip().split(\'\\t\', 1)\n        count = int(count)\n        print("{0}\\t{1}".format(word, count))\n    except ValueError as e:\n        continue')


# ## Step 3. Bash commands
# 
# <b> Hint: </b> For printing the exact row you may use basic UNIX commands. For instance, sed/head/tail/... (if you know other commands, you can use them).
# 
# To run both jobs, you must use two consecutive yarn-commands. Remember that the input for the second job is the ouput for the first job.

# In[6]:


get_ipython().run_cell_magic('bash', '', '\nOUT_DIR="assignment1_"$(date +"%s%6N")\nNUM_REDUCERS=8\n\n# Code for your first job\nhdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null\n\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -D mapred.jab.name="Streaming wordCount" \\\n    -D mapreduce.job.reduces=${NUM_REDUCERS} \\\n    -files mapper1.py,reducer1.py \\\n    -mapper "python mapper1.py" \\\n    -reducer "python reducer1.py" \\\n    -input /data/wiki/en_articles_part \\\n    -output ${OUT_DIR} > /dev/null\n\n# Code for your second job\nOUT_DIR_FINAL="wordcount_final_"$(date +"%s%6N")\nNUM_REDUCERS=1\n\nhdfs dfs -rm -r -skipTrash ${OUT_DIR_FINAL} > /dev/null\n\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \\\n    -D mapreduce.partition.keycomparator.options=-nr \\\n    -D mapred.jab.name="Streaming wordCount Rating" \\\n    -D mapreduce.job.reduces=${NUM_REDUCERS} \\\n    -files reverse_mapper1.py,reverse_reducer1.py \\\n    -mapper "python reverse_mapper1.py" \\\n    -reducer "python reverse_reducer1.py" \\\n    -input ${OUT_DIR} \\\n    -output ${OUT_DIR_FINAL} > /dev/null\n    \n\n\n# Code for obtaining the results\n#hdfs dfs -cat ${OUT_DIR}/part-00000 | sed -n \'7p;8q\'\n\n#hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null')

