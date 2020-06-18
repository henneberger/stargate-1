/*
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

export class Address {
  entityId?: string;
  street1: string;
  street2: string;
  city: string
  postalCode: string;
  country: string;
  constructor(street1: string, street2: string, city: string, postcalCode: string, country: string) {
    this.street1 = street1;
    this.street2 = street2;
    this.city = city;
    this.postalCode = postcalCode;
    this.country = country;
  }
}
export class Customer {
  entityId?: string;
  email: string;
  firstName: string;
  lastName: string;
  addresses: Array<Address> = new Array<Address>();
  orders: Array<Order> = new Array<Order>();

  constructor(email: string, firstName: string, lastName: string) {
    this.email = email;
    this.firstName = firstName;
    this.lastName = lastName;
  }
}

export class Order {
	time: number;
	subtotal: number;
	tax: number;
  entityId?: string;
  lineItems: Array<LineItem>;
  isOpen: boolean;
  deliveryAddress: Array<Address>;
  customer: Array<Customer>;

  constructor(lineItems: Array<LineItem>, isOpen: boolean) {
		this.subtotal = 0;
		this.tax = 0.0;
		this.time = new Date().getMilliseconds();
    this.lineItems = lineItems;
    this.isOpen = isOpen;
		this.deliveryAddress = new Array<Address>();
		this.customer = new Array<Customer>();
  }

}

export class LineItem {
  entityId?: string;
  product: Array<Product>;
  quantity: number;
  receiveBy?: Date;
  savedPrice?: number;

  constructor(product: Array<Product>, quantity: number) {
    this.product = product;
    this.quantity = quantity;
  }
}

export class Product {
  entityId?: string;
  name: string;
  photoUrl: string;
  desc: string;
  price: number;
  category: Array<Category> = new Array<Category>();

  constructor(name: string, photoUrl: string, desc: string, price: number ) {
    this.name = name;
    this.photoUrl = photoUrl;
    this.desc = desc;
    this.price = price;
  }
}

export class Category {
  entityId?: string;
  name: string;
  desc: string;
  products: Array<Product> = new Array<Product>();

  constructor(name: string, desc: string) {
    this.name = name;
    this.desc = desc;
  }
}
