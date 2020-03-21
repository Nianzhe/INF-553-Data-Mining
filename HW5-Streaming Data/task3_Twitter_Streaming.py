import random
import collections
import tweepy

count = 0
counter = collections.Counter()
l = [] 

class MyStreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
        global count
        global counter
        global l
        count +=1
        if count <= 150:
            l.append(status.text)
            words = status.text.split()
            for w in words:
                if w.startswith('#') and len(w) >1:
                    counter.update({w[1:]:1})
        else:
            random_num = random.randint(1, count)
            if random_num <= 150:
                old = l[random_num-1]
                words_old = old.split()
                for w in words_old:
                    if w.startswith('#') and len(w) >1:
                        counter.subtract({w[1:]:1})
                new = status.text
                l[random_num-1] = new
                words_new = new.split()
                for w in words_new:
                    if w.startswith('#') and len(w) >1:
                        counter.update({w[1:]:1})
        counter = sorted(counter.items(), key=lambda pair: [-pair[1], pair[0]])
        counter = collections.Counter(dict(counter))
        top5 = counter.most_common(5)
        print("The number of tweets with tags from the beginning: ", count)
        for x in top5:
            print(x[0]+": "+str(x[1]))
        if len(top5) >= 1:
            min_item = top5[len(top5)-1]
            for x in counter.items():
                if x[1] == min_item[1] and x not in top5:
                    print(x[0]+": "+str(x[1]))
        print("\n")
        
    def on_error(self, status_code):
        if status_code == 420:
            return False

consumer_key = "……"
consumer_secret = "……"
access_token = "……"
access_token_secret = "……"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

# try to filter the tags with some meaning
myStream.filter(languages=["en"],track=['#a','#b','#c','#d','#e','#f','#g','#h','#i','#j','#k','#l','#m','#n','#o','#p','#q','#r','#s','#t','#u','#v','#w','#x','#y','#z','#A','#B','#C','#D','#E','#F','#G','#H','#I','#J','#K','#L','#M','#N','#O','#P','#Q','#R','#S','#T','#U','#V','#W','#X','#Y','#Z'])
# myStream.filter(track=['#'])



