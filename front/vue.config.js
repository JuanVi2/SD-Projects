const { defineConfig } = require('@vue/cli-service')
module.exports = defineConfig({
  transpileDependencies: true
})

const fs = require('fs');
const path = require('path');

module.exports = {
  devServer: {
    server: {
      type: 'https',
      options: {
        key: fs.readFileSync(path.resolve(__dirname, '../AA_Engine/key.pem')),
        cert: fs.readFileSync(path.resolve(__dirname, '../AA_Engine/cert.pem'))
      }
    }
  }
};
