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

import React from "react";
import "./App.css";
import {
	Category,
	Customer,
	Order,
	Product,
	LineItem
} from "./models/Stargate";
import {
	Catalog,
	Repository,
	Mutated,
	QueryResponse
} from "./models/StargateRepository";
import { Container } from "./views/Container";

export const initialState = new Date().getMilliseconds();

export class AppProps {
	repository: Repository;
	constructor(repository: Repository) {
		this.repository = repository;
	}
}

export interface AppState {
	initialState: number;
	customer: Customer;
	products: Array<Product>;
	allProductCount: number;
	categories: Array<String>;
	searchTerm: string;
	order: Order;
	ordersVisible: boolean;
	categoryFilterValue: string;
	isOpen: boolean;
}

class App extends React.Component<AppProps, AppState> {
	root: Repository;

	constructor(props: AppProps) {
		super(props);
		this.root = props.repository;
		this.state = {
			initialState: initialState,
			isOpen: false,
			ordersVisible: false,
			allProductCount: 0,
			searchTerm: "",
			categoryFilterValue: "",
			order: new Order(new Array<LineItem>(), false),
			categories: new Array<String>(),
			customer: new Customer("", "", ""),
			products: new Array<Product>()
		};
	}

	async componentDidMount() {
		this.initialize()
	}

	async initialize(): Promise<void>{
	 	await this.root
			.initializeState()
			.then(state => this.setState(state));
		this.root.setRows((result: Array<Product>) => {
			if (result) {
				const categories = result
					.map((p: Product) => p.category[0].name)
					.filter(
						(
							n: string,
							i: number,
							self: Array<String>
						) => self.indexOf(n) === i
					)
					.sort();
				this.setState({
					allProductCount: result.length,
					products: result,
					categories: categories
				});
			}
		});
	}

	updateSearch = (ev: React.ChangeEvent<HTMLInputElement>): void => {
		const searchTerm = ev.target.value;
		const response = this.root.search(
			searchTerm,
			this.state.categoryFilterValue
		);
		response.then(result => {
			this.setState({
				products: result
			});
		});
	};

	categoryFilter = (category: String): void => {
		const response = this.root.search(
			this.state.searchTerm,
			category.toString()
		);
		response.then(result => {
			this.setState({
				products: result
			});
		});
		this.setState({
			categoryFilterValue: category.toString()
		});
	};

	initializeCatalog = () => {
		this.root.createRepo().then(success => {
			if (success) {
				const catalogPromise = this.root.initializeCatalog();
				catalogPromise.then((c: Catalog) =>
					this.setState({
						categories: c.categories
							.map(
								(x: Category) =>
									x.name
							)
							.sort(),
						products: c.products,
						allProductCount:
							c.products.length
					})
				);
			} else {
				console.log(
					"unable to create namespace veggies"
				);
			}
		});
	};

	updateLineItem = (): void => {};
	addLineItem = (entityId: string | undefined): void => {
		if (entityId === undefined) {
			console.log(
				"critical error adding item there is no entityId present"
			);
			return;
		}
		const matches = this.state.products.filter(
			x => x.entityId === entityId
		);
		if (matches.length !== 1) {
			console.log(
				"unexpected error: tried to match product with id " +
					entityId +
					" but had " +
					matches.length +
					" matches"
			);
			return;
		}
		const product = matches[0];
		const lineItem = new LineItem(new Array<Product>(product), 1);
		const order = this.state.order;
		this.root.addLineItem(order, lineItem).then(x => {
			if (x.length === 1) {
				lineItem.entityId = x[0].entityId;
				order.lineItems.push(lineItem);
				this.setState({
					order: order
				});
			} else {
				console.log(x);
			}
		});
	};

	removeLineItem = (entityId: string | undefined): void => {
		if (entityId === null || entityId === undefined) {
			return;
		}
		const order: Order = this.state.order;
		const mutated = this.root.deleteLineItem(entityId);
		mutated.then(x => {
			if (x.data.length === 1) {
				order.lineItems = order.lineItems.filter(
					x => x.entityId !== entityId
				);
				this.setState({
					order: order
				});
			}
		});
	};

	buyNow = (): void => {
		const originalOrder = this.state.order;
		if (originalOrder.entityId === undefined) {
			console.log(
				"critical error tried to empty cart and it's not setup"
			);
			return;
		}
		this.root
			.buyNow(originalOrder.entityId)
			.then((x: QueryResponse<Mutated>) =>
				x.data.map(o => o.entityId)
			)
			.then((x: Array<String>) => {
				const isClosed = x.length === 1;
				if (isClosed) {
					this.initialize()
					this.setState({
						isOpen: false,
						initialState: new Date().getMilliseconds()
					});
				} else {
					console.log(
						"error order did not close"
					);
				}
			});
	};

	showOrders = (): void => {
		this.setState({
			ordersVisible: true
		});
	};

	emptyCart = (): void => {
		const originalOrder = this.state.order;
		if (originalOrder.entityId === undefined) {
			console.log(
				"critical error tried to empty cart and it's not setup"
			);
			return;
		}
		this.root
			.clearCart(originalOrder.entityId)
			.then((x: QueryResponse<Mutated>) =>
				x.data.map(o => o.entityId)
			)
			.then((x: Array<String>) => {
				const nonDeletedItems = originalOrder.lineItems.filter(
					l =>
						l.entityId !== undefined &&
						x.indexOf(l.entityId) === -1
				);
				originalOrder.lineItems = nonDeletedItems;
				this.setState({
					order: originalOrder
				});
			});
	};
	showDialog = (isOpen: boolean): void => {
		this.setState({
			isOpen: isOpen
		});
	};

	render() {
		return (
			<Container
			 	key={this.state.initialState}
				showDialog={this.showDialog}
				isOpen={this.state.isOpen}
				categoryFilterValue={
					this.state.categoryFilterValue
				}
				categoryFilter={this.categoryFilter}
				showOrders={this.showOrders}
				emptyCart={this.emptyCart}
				buyNow={this.buyNow}
				addLineItem={this.addLineItem}
				removeLineItem={this.removeLineItem}
				updateLineItem={this.updateLineItem}
				initializeCatalog={this.initializeCatalog}
				updateSearch={this.updateSearch}
				customer={this.state.customer}
				products={this.state.products}
				allProductCount={this.state.allProductCount}
				categories={this.state.categories}
				order={this.state.order}
				searchTerm={this.state.searchTerm}
			/>
		);
	}
}

export default App;
