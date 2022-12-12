var str = `+-------------------+
| dhohirp.fullname  |
+-------------------+
| hii               |
| dhohir pradana    |
| Dhohir Pradana    |
| Dhohir Pradana    |
| Dhohir Pradana    |
| Dhohir Pradana    |
| Dhohir Pradana    |
| Dhohir Pradana    |
| Dhohir Pradana 2  |
+-------------------+`;

var splitByLines = str.split(/\r?\n/);

var alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

// splitByLines delete where not contain alphabet
splitByLines = splitByLines.filter((line) => {
  for (let i = 0; i < line.length; i++) {
    if (alphabet.includes(line[i])) {
      return true;
    }
  }
  return false;
});

// delete | at the start and end of each line
splitByLines = splitByLines.map((line) => {
  return line.slice(1, -1);
});

// delete 1 space at the start and end of string
splitByLines = splitByLines.map((line) => {
  return line.trim();
});

console.log(splitByLines);
