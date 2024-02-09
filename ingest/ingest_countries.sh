curl --request GET \
     --url 'https://api.openaq.org/v2/countries?limit=100&page=1&offset=0&sort=asc&order_by=name' \
     --header 'accept: application/json' \
     --header 'content-type: application/json'