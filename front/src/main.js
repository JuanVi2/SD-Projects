import { createApp } from 'vue';
import App from './App.vue';
import MapComponent from './components/MapComponent.vue'; // asegúrate de que esta ruta es correcta
//import Slider from './components/Slider.vue'; // asegúrate de que esta ruta es correcta

const app = createApp(App);

app.component('MapComponent', MapComponent); // registra el componente MapComponent globalmente
//app.component('Slider', Slider); // registra el componente Slider globalmente

app.mount('#app');

