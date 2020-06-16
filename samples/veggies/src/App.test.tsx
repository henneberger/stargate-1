import React from 'react';
import { render } from '@testing-library/react';
import App, { AppState } from './App';
import { Catalog, ProductResponse } from './models/StargateRepository';
import { Address, Customer, LineItem, Order, Product, Category } from './models/Stargate';

class MockRepo {
  initializeState(): AppState{
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
      allProductCount: 0,
      customer: customer,
      order: new Order(lineItems, address1, customer),
      products: new Array<Product>(),
      categories: new Array<Category>(),
      searchTerm: "",
    };
  }

  initializeCatalog(): Promise<Catalog>{
    return new Promise((resolve, reject) => {
      resolve(new Catalog(new Array<Category>(), new Array<Product>()))
    })
  }
  search(searchTerm: string): Promise<ProductResponse>{
    return new Promise((resolve, reject) => {
      resolve(new Array<Product>())
    });
  }

  setRows(action: (p: ProductResponse) => void): void{
    action(new Array<Product>())
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
