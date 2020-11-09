1. Create a new folder in hummingbot/connector/exchange for your connector:
    cd ./hummingbot/connector/exchange
    mkdir your_connector_name
  Note the use of lowercase and underscore for spaces. this is necessary

2. From hummingbot/connector/exchange, copy the contents of new_connector:

    cp ./new_connector/* your_connector_name

3. Rename the files in the folder so that the preface new_connector is replaced by your 
   connector with the same format as described in step 1.

4. Perform the following series of replacements using search and replace (restricted to your new connector's
   directory):
   i) classNewConnector -> YourConnectorName (note that your_connector_name in step 1 is converted to precisely
      YourConnectorName)
   ii) urlNEW_CONNECTOR -> YOUR_CONNECTOR_NAME (note capitalization)
   iii) url_new_connector -> your_connector_name (this is mostly done to distinguish the
        name of the connector in the urls used by the api from many other references
        addressed in step iv here. The names may be the same, but they may not, and this
        is a good opportunity to start looking at the API!)
   iv) new_connector -> your_connector_name
   In the last step here, the final name must match the name of your new connector's
   directory. This is used to import modules between the various parts of the connector.
   This will also name various instances of classes and other variables used in the connector

5.  