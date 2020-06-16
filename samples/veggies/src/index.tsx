import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import * as serviceWorker from './serviceWorker';
import { StargateRepository } from './models/StargateRepository';


const server = "http://localhost:3000";
const ns = "veggies";
const repository = new StargateRepository(server, ns);
ReactDOM.render(
  <React.StrictMode>
      <App repository={repository} />
  </React.StrictMode>,
  document.getElementById('root')
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
