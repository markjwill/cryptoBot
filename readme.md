#ReadMe

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
