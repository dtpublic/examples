This project contains applications showcasing different **Parsers** and **Formatters** present in the Malhar Library. For all the apps, parameters are configurable in META_INF/properties.xml.

* **Json Parser App**  
This app showcases **Json Parser**. Data generator sends Json data to the Json Parser which emits each record as POJO on the *output* port. The parser also has *parsedOutput* port that outputs each record as JSONObject and *error* port that emits a error records as key value pair. 