<template>
  <div id="app">
    <HeaderComponent></HeaderComponent>
    <HelloWorld msg="Welcome to Your Vue.js App"/>
    <router-view></router-view>
    <componente1></componente1>
  </div>
</template>

<script>
import Home from './components/Home.vue'
import Cabecera from './components/Cabecera.vue'

export default {
  name: 'App',
  components: {
    Home,
    Cabecera
  }
}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
