pkill tail
touch client.log
java -Xms4048m -Xmx4048m -jar ./target/dummy-client-fat.jar -c ./demo.json 2> client.log &
tail -f client.log &
