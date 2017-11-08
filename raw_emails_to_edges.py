import pandas as pd
import re
import numpy as np

nb_chunks = 1035

# This email dataset can be found at https://www.kaggle.com/wcukierski/enron-email-dataset
# It is the May 2015 version of the Enron email corpus dataset
data = pd.read_csv('emails.csv', chunksize=500)

# First run and other_runs are the same functions, except that the first one implements headers for the destination dataframe. 
# This is definitely not the cleanest way to implement it but was very straight forward.

# The function takes in a chunk (defined as 500 rows in the emails.csv file) and loops through each message to retrieve relevant information:
# It first spplits the message into sevral lines, each containing a field

# First, gets the date of the email, formats it nicely and stores it.
# Second, uses a regular expression to find email addresses ending with @enron.com within the From: and To: fields.
# It then stores those, write them in a dataframe, and write the dataframe to a csv file. 

# Each chunk is appended to the csv file, to avoid storing all the information at  once. That's the way I managed to avoid memory issues and protect from eventual crashes.


def first_run(data):
    chunk = next(data)
    # For loop here
    # messages = chunk.message
    # message = messages[0]
    dates = []
    senders = []
    receivers = []
    address = re.compile('[\w\.-]+@enron.com')

    for i, message in enumerate(chunk.message):
        try:
            lines = message.split('\n')
            date_line = lines[1]
            from_line = lines[2]
            to_line = lines[3]
        except Exception as e:
            print(e,' for ', i, '// Code 1')

            # continue

        try:
            date = date_line.replace('Date: ', '')
            date = pd.to_datetime(date)
        except Exception as e:
            print(e, ' for ', i, '// Code 2')
            date = np.nan
        finally:
            dates.append(date)

        try:
            sender_email = re.findall(address, from_line)[0]
        except Exception as e:
            print(e, ' for ', i, '// Code 3')
            sender_email = np.nan
        finally:
            senders.append(sender_email)

        try:
            first_receiver = re.findall(address, to_line)[0]
        except Exception as e:
            print(e, ' for ', i, '// Code 4')
            first_receiver = np.nan
        finally:
            receivers.append(first_receiver)

    dateseries = pd.Series(dates)
    senderseries = pd.Series(senders)
    receiverseries = pd.Series(receivers)

    intermediatedf = pd.DataFrame()
    intermediatedf['Sender'] = senderseries
    intermediatedf['Recipient'] = receiverseries
    intermediatedf['Timestamp'] = dateseries


    intermediatedf.dropna(inplace=True)

    intermediatedf.to_csv('Edges.csv')
    print(intermediatedf.shape)
    print('First chunk done.')

def other_runs(data):
    count = 0
    for chunk in data:
        count+=1
        if count == 1:
            continue

        else:
            dates = []
            senders = []
            receivers = []
            address = re.compile('[\w\.-]+@enron.com')

            for i, message in enumerate(chunk.message):
                try:
                    lines = message.split('\n')
                    date_line = lines[1]
                    from_line = lines[2]
                    to_line = lines[3]
                except Exception as e:
                    print(e, ' for ', i, '// Code 1')

                    # continue

                try:
                    date = date_line.replace('Date: ', '')
                    date = pd.to_datetime(date)
                except Exception as e:
                    print(e, ' for ', i, '// Code 2')
                    date = np.nan
                finally:
                    dates.append(date)

                try:
                    sender_email = re.findall(address, from_line)[0]
                except Exception as e:
                    print(e, ' for ', i, '// Code 3')
                    sender_email = np.nan
                finally:
                    senders.append(sender_email)

                try:
                    first_receiver = re.findall(address, to_line)[0]
                except Exception as e:
                    print(e, ' for ', i, '// Code 4')
                    first_receiver = np.nan
                finally:
                    receivers.append(first_receiver)

            dateseries = pd.Series(dates)
            senderseries = pd.Series(senders)
            receiverseries = pd.Series(receivers)

            intermediatedf = pd.DataFrame()
            intermediatedf['Sender'] = senderseries
            intermediatedf['Recipient'] = receiverseries
            intermediatedf['Timestamp'] = dateseries

            intermediatedf.dropna(inplace=True)

            with open('Edges.csv', 'a') as outfile:
                intermediatedf.to_csv(outfile, header=False)
            print(count, '/ 1035 done.')

first_run(data)
other_runs(data)

