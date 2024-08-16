##1 Datasets

Download datasets from https://star.cs.ucr.edu/.

Five datasets are used in our experiments: 
1) Parks contains 10 million boundaries of parks and green areas from all over the world; 
2) Lakes contains 8 million water areas in the world; 
3) Buildings contains 114 million building outlines in the world; 
4) Roads contains 72 million roads and streets around the world; 
5) POIs contains 147 million points of interest around the world.

Process datasets into the following format:`ID\tWKT`.

For example: `1   POINT (26.0883793 44.4078777)`

##2 Config

Configuring the "config/partitionerTest.json" file.

Configuring the "bin/partitionerTest.sh" file.

##3 Execution
`bash ./bin/partitionerTest.sh ./config/partitionerTest.json`