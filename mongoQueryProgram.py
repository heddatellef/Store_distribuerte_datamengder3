from datetime import datetime, timedelta
from pprint import pprint

from bson import SON
from haversine import haversine, Unit
from tabulate import tabulate

from DbConnector import DbConnector


class MongoQueryProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    # How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
    def query1(self):
        print('Solution to task 1:\n')
        collections = ["User", "Activity", "TrackPoint"]
        for collection in collections:
            docs = self.db[collection].count_documents({})
            print(collection, ':', docs)

    # Find the average, minimum and maximum number of activities per user.
    def query2(self):
        print('\nSolution to task 2:\n')
        commands = ['$max', '$min', '$avg']
        for command in commands:
            docs = self.db['User'].aggregate([
                {
                    '$group': {
                        '_id': 0,
                        'command': {command: {'$size': '$activities_ids'}}
                    }
                }
            ])
            print(command, ' number of activities:')
            pprint(list(docs)[0]['command'])

    # Find the top 10 users with the highest number of activities.
    def query3(self):
        print('\nSolution to task 3:\n')
        docs = self.db['User'].aggregate([
            {
                '$project': {
                    '_id': '$_id',
                    'activities': {'$size': '$activities_ids'}
                }
            },
            {
                '$sort': {'activities': -1}
            },
            {
                '$limit': 10
            }
        ])
        print('Top ten users:')
        pprint(list(docs))

    # Find the number of users that have started the activity in one day and ended the activity the next day.
    def query4(self):
        print('Solution to task 4:\n')
        result = self.db['Activity'].aggregate(
            [
                {
                    "$project": {
                        "_id": "$user_id",
                        "start": {'$dayOfYear': ['$start_date_time']},
                        "end": {'$dayOfYear': ['$end_date_time']},
                    }
                },
                {
                    '$match': {
                        '$expr': {'$and': [{'$subtract': ['$end', '$start']}, 1]}
                    },
                },
                {
                    '$group': {
                        "_id": "$_id",
                    }
                },
                {
                    "$count": "count"
                }

            ])
        print('Number of overnight users:')
        pprint(list(result)[0]['count'])
        # In this query we are not taking into account activities that are done over new years eve.
        # We didn't find this extra work worth the time, since newer versions of mongoDB have the built in function
        # "$dateDiff" which calculates the difference between two dates in number of days.

    # Find activities that are registered multiple times. You should find the query even if you get zero results.
    def query5(self):
        print('\nSolution to task 5:\n')
        result = self.db["Activity"].aggregate(
            [
                {
                    "$group": {
                        "_id": {
                            "transportation_mode": "$transportation_mode",
                            "end_date_time": "$end_date_time",
                            "user_id": "$user_id",
                            "start_date_time": "$start_date_time"
                        },
                        "COUNT(*)": {
                            "$sum": 1
                        }
                    }
                },
                {
                    "$project": {
                        "user_id": "$_id.user_id",
                        "transportation_mode": "$_id.transportation_mode",
                        "start_date_time": "$_id.start_date_time",
                        "end_date_time": "$_id.end_date_time",
                        "COUNT(*)": "$COUNT(*)",
                        "_id": 0
                    }
                },
                {
                    "$match": {
                        "COUNT(*)": {
                            "$gt": 1
                        }
                    }
                }
            ]
        )
        print('Activities registered multiple times:')
        pprint(list(result))

    # An infected person has been at position (lat, lon) (39.97548, 116.33031) at
    # time ‘2008-08-24 15:38:00’. Find the user_id(s) which have been close to this
    # person in time and space (pandemic tracking).
    # Close is defined as the same minute (60 seconds) and space (100 meters).
    # (This is a simplification of the “unsolvable” problem given i exercise 2).
    def query6(self):
        print('\n\nSolution to task 6:\n')

        # Finds all the trackpoints in the database, and extracts the user_id, lat, lon, and date_time.
        # Our query takes very long time, so we have had a tough time testing this task
        # Therefore, we created a test user to check if the code works as intended
        result = self.db["Activity"].aggregate(
            [
                {'$lookup': {
                    'from': 'TrackPoint',
                    'localField': "_id",
                    'foreignField': "activity_id",
                    'as': 'trackPoint'
                }
                },
                {'$project': {
                    'user_id': '$user_id',
                    'lat': '$trackPoint.lat',
                    'lon': '$trackPoint.lon',
                    'date_time': '$trackPoint.date_time'
                }
                },
                {
                    '$limit': 1
                }
            ])

        # Create some variables for the infected person for checking
        infected_person_pos = (39.97548, 116.33031)
        infected_person_time = datetime.strptime("2008-08-24 15:38:00", '%Y-%m-%d %H:%M:%S')
        # Create a minute timespace before and after the time.
        inf_time_before = infected_person_time - timedelta(seconds=60)
        inf_time_after = infected_person_time + timedelta(seconds=60)

        # Since the query took so long we created an identical replica of the result from the query
        # with data that would give a result.
        test_user_time = datetime.strptime("2008-08-24 15:38:40",
                                           '%Y-%m-%d %H:%M:%S')  # 40 seconds after the infected time
        test_user = [{
            '_id': 1,
            'user_id': '000',
            'lat': [39.97543],  # close latitude
            'lon': [116.33032],  # close longitude
            'date_time': [test_user_time]
        }]

        # Create a list of the result from the query
        tp_list = list(test_user)  # Change this variable to "result" to start the original query

        # Also create an array of the users the code finds
        close_users = []

        # Iterates through the activities the query finds
        for i, currDict in enumerate(tp_list):
            user_id = currDict['user_id']

            # Iterates through every lat, lon, and date_time in the activity
            for tp in range(len(currDict['lat'])):  # lat, lon, and date_time is the same length, just chose one

                # Create variables for the position and date -> to check the distance in space and time
                lat = currDict['lat'][tp]
                lon = currDict['lon'][tp]
                pos = (lat, lon)
                date_time = currDict['date_time'][tp]
                distance = haversine(pos, infected_person_pos, unit=Unit.METERS)

                # If a user has been nearby at within 100 meters and 60 seconds, add the user_id to the array.
                # By running query6(), you can clearly see that the user is added, and the if statement works as intended
                if distance <= 100 and date_time > inf_time_before and date_time < inf_time_after and user_id not in close_users:
                    close_users.append(user_id)

        # Prints the array
        print("Check if the testuser '000' has been nearby:")
        print("Close users:", close_users)

    # Find all users that have never taken a taxi.
    def query7(self):
        print('\nSolution to task 7:\n')
        # Starting by finding the taxi users
        taxi_users = self.db["Activity"].aggregate(
            [
                {
                    "$match": {
                        "transportation_mode": "taxi"
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "user_id": "$user_id"
                    }
                },
                {
                    "$group": {
                        "_id": 0,
                        "distinct": {
                            "$addToSet": "$$ROOT"
                        }
                    }
                }
            ])
        taxi_users = list(taxi_users)[0]['distinct']

        # putting the taxi users in an array
        taxi_array = []
        for taxi_user in taxi_users:
            taxi_array.append(taxi_user["user_id"])

        # finding all users
        all_users = self.db["User"].aggregate(
            [
                {
                    "$project": {
                        "_id": "$_id",
                    }
                }
            ])
        all_users = list(all_users)

        # if a user is not in the taxi array, add the id of the user into the non-taxi users' array
        # as well as printing their id
        non_taxi_users = []
        print('Not taxi users:')
        for i in range(len(all_users)):
            if all_users[i]['_id'] not in taxi_array:
                non_taxi_user = all_users[i]['_id']
                non_taxi_users.append(non_taxi_user)
                print(non_taxi_user)

    # Find all types of transportation modes and count how many distinct users that
    # have used the different transportation modes. Do not count the rows where the
    # transportation mode is null.
    def query8(self):
        print('\nSolution to task 8:\n')
        result = self.db["Activity"].aggregate(
            [
                {
                    '$match': {'transportation_mode': {'$exists': True}}
                },
                {
                    "$group": {
                        "_id": {
                            "transportation_mode": "$transportation_mode",
                        },
                        "distinct_count": {"$addToSet": "$user_id"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "TransportationMode": "$_id.transportation_mode",
                        "NumberOfUsers": {"$size": "$distinct_count"}
                    }
                },
                {
                    "$sort": {"NumberOfUsers": -1}
                },
            ])
        print('Types of transportation and number of their distinct users:')
        result = list(result)
        print(tabulate(result))

    # a) Find the year and month with the most activities.
    def query9(self):
        print('\nSolution to task 9a:\n')
        result_a = self.db["Activity"].aggregate(
            [
                {
                    "$group": {
                        "_id": {
                            "year_month": {'$substr': ['$start_date_time', 0, 7]},
                        },
                        "users": {
                            "$sum": 1
                        }
                    }
                },
                {
                    '$sort': {'users': -1}
                },
                {
                    '$limit': 1
                },
            ]
        )

        top_ym = list(result_a)
        most_pop = str(top_ym[0]['_id']['year_month'])

        # Saving the top year and month in separate variables to make task 9b easier.
        mp_year = int(most_pop[0:4])
        mp_month = int(most_pop[5:])
        print('Most popular year and month:\n', mp_year, '-', mp_month)

        # b) Which user had the most activities this year and month, and how many recorded hours do they have?
        # Do they have more hours recorded than the user with the second most activities?
        print('\n\nSolution to task 9b:\n')
        result_b1 = self.db["Activity"].aggregate(
            [
                {
                    "$project": {
                        "_id": "$user_id",
                        "year": {'$year': ['$start_date_time']},
                        "month": {'$month': ['$start_date_time']},
                    }
                },
                {
                    '$match': {
                        'year': mp_year,
                        'month': mp_month}
                },
                {
                    '$group': {
                        '_id': '$_id',
                        'activities': {'$sum': 1}
                    }
                },
                {
                    '$sort': {'activities': -1}
                },
                {
                    '$limit': 2
                }
            ]
        )
        # Setting the limit to 2 in order to get both the user with the most activities,
        # and the user with the second most activities.
        print('Top 2 users and number of activities:')
        result_b1 = list(result_b1)
        print(tabulate(result_b1))

        # Saving the top 2 ids for the next query
        id1 = result_b1[0]['_id']
        id2 = result_b1[1]['_id']

        result_b2 = self.db["Activity"].aggregate(
            [
                {
                    "$project": {
                        "_id": "$user_id",
                        "year": {'$year': ['$start_date_time']},
                        "month": {'$month': ['$start_date_time']},
                        # Dividing by 3 600 000 to get the subtracting returns the ISOTime difference in milliseconds
                        "hours": {
                            "$trunc": [{"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]},
                                       2]}
                    }
                },
                {
                    '$match': {
                        '$and': [
                            {'year': mp_year},
                            {'month': mp_month},
                            {'$or': [{'_id': id1}, {'_id': id2}]}
                        ]
                    },
                },
                {
                    '$group': {
                        '_id': '$_id',
                        'hours': {'$sum': '$hours'}
                    }
                },
                {
                    '$sort': {'hours': -1}
                }
            ]
        )

        print('\nHours recorded for these two users:')
        print(tabulate(list(result_b2)))

    # Find the total distance (in km) walked in 2008, by user with id=112.
    def query10(self):
        print('\nSolution to task 10:\n')
        tp_list = self.db['Activity'].aggregate([
            {'$match': {
                '$and': [
                    {'user_id': '112'},
                    {'transportation_mode': 'walk'},
                    {'$expr': {'$and': [{'$year': '$end_date_time'}, '2008']}}
                ]
            }
            },
            {'$lookup': {
                'from': 'TrackPoint',
                'localField': "_id",
                'foreignField': "activity_id",
                'as': 'trackPoint'
            }
            },
            {'$project': {
                'lon': '$trackPoint.lon',
                'lat': '$trackPoint.lat',
            }}
        ])

        tp_list = list(tp_list)

        total_distance = 0
        for activity in tp_list:
            number_of_tp = len(activity['lon'])
            for j in range(0, number_of_tp - 1):
                lon = activity['lon'][j]
                lat = activity['lat'][j]
                total_distance += haversine((lat, lon), (activity['lat'][j + 1], activity['lon'][j + 1]))
        print('Distance walked by user 112 in 2008:\n', round(total_distance), 'km')

    # Find the top 20 users who have gained the most altitude meters.
    def query11(self):
        print('\nSolution to task 11:\n')
        altitudes = self.db['Activity'].aggregate([
            {'$lookup': {
                'from': 'TrackPoint',
                'localField': "trackPoint_ids",
                'foreignField': "_id",
                'as': 'trackPoint'
            }},
            {'$project': {
                'activity_id': '$_id',
                'altitude': '$trackPoint.altitude',
                'user_id': '$user_id'
            }}
        ])

        altitudes = list(altitudes)

        user_altitudes = {}
        for activity in altitudes:
            user_id = activity['user_id']
            if user_id not in user_altitudes:
                user_altitudes[user_id] = 0
            for i in range(len(activity['altitude']) - 1):
                curr_alt = activity['altitude'][i]
                next_alt = activity['altitude'][i + 1]
                diff_alt = (next_alt - curr_alt)
                if diff_alt > 0:
                    # adding the altitude difference in meters
                    user_altitudes[user_id] += round(diff_alt * 0.3048)

        # rounding the altitudes
        for key in user_altitudes.keys():
            user_altitudes[key] = round(user_altitudes[key])

        # sorting the altitudes in descending order
        sorted_list = sorted(user_altitudes.items(), key=lambda x: x[1], reverse=True)

        # choosing the top 20 altitudes
        top_20 = sorted_list[:20]
        print('Top 20 users in gained altitude:\n', tabulate(top_20, headers=['User', 'Altitude']))

    # Find all users who have invalid activities, and the number of invalid activities per user
    def query12(self):
        print('\n\nSolution to task 12:\n')
        activities = self.db['Activity'].aggregate([
            {'$lookup': {
                'from': 'TrackPoint',
                'localField': "trackPoint_ids",
                'foreignField': "_id",
                'as': 'trackPoint'
            }},
            {'$project': {
                'activity_id': '$_id',
                'date_time': '$trackPoint.date_time',
                'user_id': '$user_id'
            }}
        ])

        result = list(activities)
        user_invalids = {}
        for activity in result:
            user_id = activity['user_id']
            if user_id not in user_invalids:
                user_invalids[user_id] = 0
            date_times = activity['date_time']
            for i in range(len(date_times) - 1):
                curr_time = date_times[i]
                next_time = date_times[i + 1]
                diff_time = (next_time - curr_time).seconds
                # checking if the time difference is more than 5 minutes = 300 seconds
                if diff_time > 300:
                    user_invalids[user_id] += 1

        user_sorted = sorted(user_invalids.items(), key=lambda x: x[0])
        print(tabulate(user_sorted, headers=['User', 'Invalid activities']))


def main():
    program = None
    try:
        program = MongoQueryProgram()
        # program.query1()
        # program.query2()
        # program.query3()
        # program.query4()
        # program.query5()
        # program.query6()
        # program.query7()
        # program.query8()
        # program.query9()
        # program.query10()
        # program.query11()
        # program.query12()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
