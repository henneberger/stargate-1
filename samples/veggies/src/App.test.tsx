/**
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import React from 'react';
import { render } from '@testing-library/react';
import App, { AppState } from './App';
import { Catalog, QueryResponse, Mutated} from './models/StargateRepository';
import { Address, Customer, LineItem, Order, Product, Category } from './models/Stargate';

class MockRepo {
async initializeState(): Promise<AppState> {
    const address1 = new Address(
      "33 jones st",
      "apt 2",
      "Anytown",
      "84111",
      "USA"
    );
    const customer = new Customer("my@example.com", "John", "Smith");
    customer.addresses = new Array<Address>(address1);
    const lineItems = new Array<LineItem>();
    return {
      ordersVisible: false,
      isOpen: false,
      initialState: 0,
      categoryFilterValue: "",
      allProductCount: 0,
      customer: customer,
      order: new Order(lineItems, true),
      products: new Array<Product>(),
      categories: new Array<String>(),
      searchTerm: "",
    };
  }

  initializeCatalog(): Promise<Catalog>{
    return new Promise((resolve, _) => {
      resolve(new Catalog(new Array<Category>(), new Array<Product>()))
    })
  }
  
  async search(_: string): Promise<Array<Product>>{
	 return new Array<Product>();
  }

  setRows(action: (p: Array<Product>) => void): void{
    action(
	    new Array<Product>()
    );
  }
  buyNow(_: string):Promise<QueryResponse<Mutated>>{
	  return new Promise((resolve, _)=>{
		  resolve();
	  })
  }
  createRepo(): Promise<Boolean> {
	  return new Promise((resolve, _)=>{
		  resolve(true);
	  })
  }
  
  addLineItem(_: Order, lineItem: LineItem): Promise<Array<Mutated>>{
	  console.log(lineItem);
	  return new Promise((resolve, _)=>{
		  resolve();
	  })
  }

  deleteLineItem(_: string): Promise<QueryResponse<Mutated>>{
	return new Promise((resolve, _)=>{
		resolve();
	})
  }

  clearCart(_: string): Promise<QueryResponse<Mutated>>{
	return new Promise((resolve, _)=>{
		resolve();
	})
  }

}

test('component can render when products are empty', () => {
  const repo = new MockRepo()
  const { getByText } = render(
      <App repository={repo} />
    );
  const namespace = getByText(/Initialize Catalog/i);
  expect(namespace).toBeInTheDocument();
  const servername = getByText(/Veggies/i);
  expect(servername).toBeInTheDocument();
});
