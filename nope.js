const fs = require("fs");
const output = [];

for (let temp = -99.9; temp <= 99.9; temp += 0.1) {
  const tempStr = temp.toFixed(1);
  output.push(`"${tempStr}" => ${tempStr},`);
}

fs.writeFileSync("output.txt", output.join("\n"), "utf8");
