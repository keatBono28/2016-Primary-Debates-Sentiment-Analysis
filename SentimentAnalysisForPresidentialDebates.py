# Add All Imports
import csv
import re
import glob
import string
import pandas as pd
from pandas import ExcelWriter
import os
import pyodbc
import operator
import datetime



# Define all Global: Lists, Arrays, Variables
positiveWords = []
negativeWords = []
assertiveWords = []
passiveWords = []
republicanWords = []
democraticWords = []
debateData = []
allDebateText = []
canadidates = ["Bush", "Carson", 'Chafee', 'Christie', 'Clinton', 'Cruz', 'Fiorina', 'Gilmore', 'Graham', 'Huckabee', 'Jindal', 'Kasich', "O'Malley", 'Pataki', 'Paul', 'Perry', 'Rubio', 'Sanders', 'Santorum', 'Trump', 'Walker', 'Webb']
locations = ['Milwaukee, Wisconsin','Miami, Florida','Charleston, South Carolina','Greenville, South Carolina','Cleveland, Ohio','Boulder, Colorado','Las Vegas, Nevada','Manchester, New Hampshire','North Charleston, South Carolina','Flint, Michigan','Brooklyn, New York','Des Moines, Iowa','Durham, New Hampshire','Detroit, Michigan','Houston, Texas','Simi Valley, California']
# SQL Information
sqlStatement = "INSERT INTO PresidentialDebates (SpeakerName,PositiveScore,NegativeScore,Pos_NegScore,AssertiveScore,PassiveScore,Assert_PassScore,RepublicanScore,DemocraticScore,Rep_DemScore,Party,Date,DebateLocation,URL,text) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
deleteDataStatement = "DELETE FROM PresidentialDebates"
server = 'localhost'
database = 'MKTSentimentAnalysis'
username = 'BONOMO-DESKTOP\kybon'
password = ''
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=localhost;DATABASE=MKTSentimentAnalysis;Trusted_Connection=yes')
cursor = cnxn.cursor()

with open('Lexicon/positive.txt', 'r') as newFile:
    positiveWords = newFile.read().split('\n')
    print("Positive Words in...")
with open('Lexicon/negative.txt', 'r') as newFile:
    negativeWords = newFile.read().split('\n')
    print("Negative Words in...")
with open('Lexicon/assertive.txt', 'r') as newFile:
    assertiveWords = newFile.read().split('\n')
    print("Assertive Words in...")
with open('Lexicon/passive.txt', 'r') as newFile:
    passiveWords = newFile.read().split('\n')
    print("Passive Words in....")
with open('Lexicon/republican.txt', 'r') as newFile:
    republicanWords = newFile.read().split('\n')
    print("Republican Words in...")
with open('Lexicon/democratic.txt', 'r') as newFile:
    democraticWords = newFile.read().split('\n')
    print("Democratic Words in...")


# Reading in Data from Debates
def performAnalysis():
    insertCounter = 0
    skipCount = 0
    print("Reading in debates...")
    f = open('PrimaryDebates.csv')
    debateData = csv.reader(f)
    for row in debateData:
        if (any(name in str(row[1]) for name in canadidates)) and (str(row[1])!="Sanders (VIDEO)") and (str(row[1])!="Cruz (VIDEO)") and (str(row[1])!="Cuomo (VIDEO)") and (str(row[1])!="Kasich (VIDEO)") and (str(row[1])!="Ramos (VIDEO)") and (str(row[1])!="Clinton (VIDEO)") and (str(row[1])!="Fiorina (VIDEO)") and (str(row[1])!="Rubio (VIDEO)") and (str(row[1])!="Salinas (TRANSLATED)") and (str(row[1])!="Trump (VIDEO)") and (str(row[1])!="Ramos (TRANSLATED)") and (str(row[1])!="Perry (VIDEO)") and (str(row[1])!="QUESTION (TRANSLATED)") and (str(row[1])!="O'Reilly (VIDEO)") and (str(row[1])!="UNKOWN (TRANSLATED)"):
            # Reset all counters for new row of text
            posCounter = 0
            negCounter = 0
            assertiveCounter = 0
            passiveCounter = 0
            repCounter = 0
            demCounter = 0
            # Reset all total scores for new row of text
            Pos_NegScore = 0
            Assert_PassScore = 0
            Rep_DemScore = 0
            # Get the new row of text
            speakerText = row[2]
            # Clean the textual data from speaker
            speakerText = speakerText.lower().strip()
            speakerText = re.sub("<.,*?>", "", speakerText)
            # Add each word to the debate data array to help generate word cloud
            allDebateText.append(speakerText)
            # start counters
            for word in speakerText.split(" "):
                # Positive vs Negative
                if word in positiveWords:
                    posCounter = posCounter + 1
                elif word in negativeWords:
                    negCounter = negCounter + 1
                # Assertive vs Passive    
                if word in assertiveWords:
                    assertiveCounter = assertiveCounter + 1
                elif word in passiveWords:
                    passiveCounter = passiveCounter + 1
                # Republican vs Democratic
                if word in republicanWords:
                    repCounter = repCounter + 1
                elif word in democraticWords:
                    demCounter = demCounter + 1
            # Calculate total score for each 
            Pos_NegScore = posCounter - negCounter
            Assert_PassScore = assertiveCounter - passiveCounter
            Rep_DemScore = repCounter - demCounter
            # Insert All Data and Scores into database
            with cursor.execute(sqlStatement,row[1],posCounter,negCounter,Pos_NegScore,assertiveCounter,passiveCounter,Assert_PassScore,repCounter,demCounter,Rep_DemScore,row[4],row[3],row[5],row[6],row[2]):
                insertCounter = insertCounter+1
        else:
            skipCount = skipCount+1

    print(str(insertCounter) + " rows inserted")
    print(str(skipCount) + " rows skipped")

def deleteOldData():
    with cursor.execute(deleteDataStatement):
        print("Old data deleted from database")


def sentimentOverall():
    sql = "SELECT SpeakerName, sum(Pos_NegScore) as Pos_Neg_Score, sum(Assert_PassScore) as Assertive_Passive_Score, sum(Rep_DemScore) as Rep_Dem_Score FROM PresidentialDebates GROUP BY SpeakerName ORDER BY Pos_Neg_Score DESC"
    df = pd.read_sql(sql,cnxn)
    dfV1 = df.set_index("SpeakerName", drop=True)
    print(" --------------------------------------------------------------------------")
    print("                               Overall Scores                              ")
    print("                                                                           ")
    print(dfV1)


def sentimentByLocation():
    for city in locations:
        sql = "SELECT SpeakerName, sum(Pos_NegScore) as Pos_Neg_Score, sum(Assert_PassScore) as Assertive_Passive_Score, sum(Rep_DemScore) as Rep_Dem_Score FROM PresidentialDebates WHERE (DebateLocation='"+city+"') GROUP BY SpeakerName ORDER BY Pos_Neg_Score DESC"  
        df = pd.read_sql(sql,cnxn)
        dfV1 = df.set_index("SpeakerName", drop=True)
        print(" --------------------------------------------------------------------------")
        print("                  Scores in "+str(city)+"                                  ")
        print("                                                                           ")
        print(dfV1)   
        print(" --------------------------------------------------------------------------")
        print("                                      ")
        print("                                      ")
        print("                                      ")
        print("                                      ")


def sentimentByParty():
    sql = "SELECT Party, sum(Pos_NegScore) as PosNegScore, sum(Assert_PassScore) as AssPassScore, sum(Rep_DemScore) as RepDemScore FROM PresidentialDebates GROUP BY Party ORDER BY PosNegScore DESC"
    df = pd.read_sql(sql,cnxn)
    dfV1 = df.set_index("Party", drop=True)
    print(" --------------------------------------------------------------------------")
    print("                         Overall Scores by Party                           ")
    print("                                                                           ")
    print(dfV1)

    
def writeTextToFile():
    with open('speakerText.csv', 'w', newline='', encoding='UTF-8') as myFile:
        writer = csv.writer(myFile)
        writer.writerow(allDebateText)
        
# Main Function
def main():
    # Delete old data from tables
    deleteOldData()
    print("Lexicons have been read in...")
    # Call the sentiment analysis function
    print("Performing analysis...")
    performAnalysis()
    print("Analysis complete...")
    print(" --------------------------------------------------------------------------")
    print("                                      ")
    print("                                      ")
    print("                                      ")
    print("                                      ")
    sentimentOverall()
    print(" --------------------------------------------------------------------------")
    print("                                      ")
    print("                                      ")
    print("                                      ")
    print("                                      ")
    sentimentByLocation()
    sentimentByParty()
    writeTextToFile()
main()
