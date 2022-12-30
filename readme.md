#ReadMe

##Steps:

1. Collect data & Try hand written algorythm
	I ran a python script 24/7 from March 2021 to November 2022 connecting to an exchange via API with python.  It collected every trade that happened on the exchage I used for the bot.
	I also created a hand written algorythm that would predict the price and has that prediction on ~150 computed parameters storing the average, low, high, start and end prices, the percent and real change in price and the volume of trade in taht time box.
	This method of trading was almost successful in that it could trade for a positive gain most of the time, however on a long timer horizon it could not consistently trade in such a way that the overall value of the acount rose.  It took me a while to computer the proper value change over time in part because the exchange I used had a 3rd currency used to pay it's transaction fees, so I had automated teh occasion al buying of that currency as well, and eventually creaded a conversion of all 3 currencies in the account to USD available at all times.
	In real time I only computed the ~150 parapmeter for the most recent trade each time I queried the exchange api for trade data.  This created a scenaro where I needed to create another script that would run the background and computer the additional parameters for every trade inbetween each "latest trade".  I also created a similar script that would run in the background and computer and store the predition targets at various points "in the future" from the trade in each record.
2. Prep data for modeling
	I created a new table and some additional computed features converting time into time of day, day of week and day of year, each of whihc has 2 features, one with a sin wave and one with a cos wave.  I also subtraced the price of the record from all the price related features essentually making them price neutral and instead describing the real positive or negative movement for each feature.
	I also used and SKLearn normalizer to normalize all my records, being sure to keep the positive values positive and the negative values neagtive post normalization.
3. Train model
4. Verify model accuracy / usefulness in real time trading
5. Impliment trained model into real time trading

Possible next steps:
1. Create an exchange simulator and train a 2nd algorythm to decided what actions to take based on the predicted price points of the price prediction model.  Thie model would predic 2 discrete actions and a price to take action at, wait or place/update  buy / sell at price. 

## MySql Database setup:

install mariaDb ( aka mysql )

`sudo apt-get install mariadb-server`

open mysql:

`sudo mysql -uroot -p`

Create a new database: ( Make sure this matches what is in credentials.py )
`CREATE DATABASE <dbName>;`

Now, create the new user:
`CREATE USER '<dbUser>'@'localhost' IDENTIFIED BY '<dbPassword>';`
Then, allow this new user to do anything on the database we just created:
`GRANT ALL PRIVILEGES ON <dbName>.* TO '<dbUser>'@'localhost';`
Finally, reload the permissions with:
`FLUSH PRIVILEGES;`

Add db credentials file for backups credentials
`nano .my.cnf`
set content:
`[mysqldump]
user=<dbUser>
password=<dbPassword>`

Run to import from backup
`mysql -u <dbUser> -p < dbBackup.sql.gz`
Or Run to import from Schema ( empty )
`mysql -u <dbUser> -p < MrBot-Schema.sql`
Either command will ask for your <dbPassword>

## python requirements:

`sudo apt-get install python3-pip screen redis`

`pip3 install mariadb pytz redis numpy plotext`

# Necessary Diagram
```mermaid
graph LR
subgraph Predict future price
	Q[Make predictions at several times<br> in the future based on realtime<br>computed features of recent price<br> history and time]
N(Linear Regression Model -- In Developement)
O(Handwritten Algorithm -- depreciated)
end
subgraph Make decisions
	QQ[Decide to create, replace, <br>or cancel, buy or sell orders<br> and at what price]
B(Machine Learning Model -- future plan)
P(Handwritten Algorithm)
end
A{BOT} 
D(accountProvider)
F[accountSource ]
I[dataSource]
J(Excahnge)
K(Simulation w/Mysql -- future plan)
R(Debug Output)
G(Exchange)
H(Simulation w/Mysql -- future plan)
C(dataProvider)
C1(Data Compute)
C2(Data Clean)
C3(Data Normalization)
D --> F
N --> P
A --> N
P --> A
A --> C
C --> C1
C1 --> C2
C2 --> C3
C3 --> N
A --> R
C --> A
F --> D
A --> D
D --> A
C --> I
I --> C
F <-- OR --> G
F <-- OR --> H
I <-- OR --> J
I <-- OR --> K
classDef orange fill:#f96
class Q orange
class QQ orange
```

Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart
Something Under Chart







