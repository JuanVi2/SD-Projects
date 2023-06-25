<template lang="html">
    <div class="">  
        <p>
            {{ message }}
        </p>
    </div>
</template>
  
  <script>
  export default {
    name: 'HomePage',
    data() {
      return {
        message: 'Welcome to Your Vue.js App'
      }
    }
  }
  </script>
  
  <!-- Add "scoped" attribute to limit CSS to this component only -->
  <style scoped>
  h3 {
    margin: 40px 0 0;
  }
  ul {
    list-style-type: none;
    padding: 0;
  }
  li {
    display: inline-block;
    margin: 0 10px;
  }
  a {
    color: #42b983;
  }
  </style>
  