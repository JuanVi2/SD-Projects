import App from './App.vue'
import Vue from 'vue';
import helmet from "helmet";
import VueRouter from 'vue-router';


Vue.use(VueRouter);
App.use(helmet());

const routes = [
    {path: '/', component: map},
    {path: '/data', component: Slider}
  ];

const router = new VueRouter({
    routes,
    mode: 'history'
  });
  
new Vue({
    router,
    render: h => h(App),
}).$mount('#app')