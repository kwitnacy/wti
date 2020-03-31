from locust import HttpLocust, TaskSet, between


data1 = {
    "userID": 78,
    "movieID": 903,
    "rating": 4,
    "date_day": 29,
    "date_month": 10,
    "date_year": 2006,
    "date_hour": 23,
    "date_minute": 17,
    "date_second": 16,
    "Action": 0,
    "Adventure": 0,
    "Animation": 0,
    "Children": 0,
    "Comedy": 0,
    "Crime": 0,
    "Documentary": 0,
    "Drama": 1,
    "Fantasy": 0,
    "Film-Noir": 0,
    "Horror": 0,
    "IMAX": 0,
    "Musical": 0,
    "Mystery": 1,
    "Romance": 1,
    "Sci-Fi": 0,
    "Short": 0,
    "Thriller": 1,
    "War": 0,
    "Western": 0
}


def login(l):
    l.client.get('http://127.0.0.1:5000/test')

def logout(l):
    l.client.get('http://127.0.0.1:5000/ratings')

def index(l):
    l.client.post('http://127.0.0.1:5000/rating', json=json.dumps(data1))

def profile(l):
    l.client.delete('http://127.0.0.1:5000/ratings')

class UserBehavior(TaskSet):
    tasks = {index: 2, profile: 1}

    def on_start(self):
        login(self)

    def on_stop(self):
        logout(self)

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    wait_time = between(5.0, 9.0)