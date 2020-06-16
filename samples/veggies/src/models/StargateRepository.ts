import {
	Category,
	Product,
	Customer,
	Order,
	Address,
	LineItem
} from "./Stargate";
import { AppState, initialState } from "../App";

export interface Continue {
	continueId: string | undefined;
}

export interface QueryResponse<T> {
	continue: Continue;
	data: Array<T>;
}

export interface MutationResponse {
	data: Array<Mutated>;
}

export interface Mutated {
	entityId: string;
}

export class Catalog {
	categories: Array<Category>;
	products: Array<Product>;
	constructor(categories: Array<Category>, products: Array<Product>) {
		this.categories = categories;
		this.products = products;
	}
}

export interface Repository {
	buyNow(orderEntityId: string): Promise<QueryResponse<Mutated>>;
	createRepo(): Promise<Boolean>;
	addLineItem(order: Order, lineItem: LineItem): Promise<Array<Mutated>>;
	deleteLineItem(entityId: string): Promise<QueryResponse<Mutated>>;
	clearCart(orderId: string): Promise<QueryResponse<Mutated>>;
	initializeState(): Promise<AppState>;
	initializeCatalog(): Promise<Catalog>;
	search(
		searchTerm: string,
		categoryFilter: string
	): Promise<Array<Product>>;
	setRows(action: (p: Array<Product>) => void): void;
}

const jsonHeaders = {
	Accept: "application/json",
	"Content-Type": "application/json"
};

export interface ParentMutation {
	entityId: string;
	products: Array<Mutated>;
}

export interface CategoryMutationResponse {
	data: Array<ParentMutation>;
}

export class StargateRepository {
	ns: string;
	server: string;
	productsUrl: string;
	categoriesUrl: string;
	lineItemUrl: string;
	orderUrl: string;
	entitiesUrl: string;

	constructor(server: string, ns: string) {
		this.server = server;
		this.ns = ns;
		this.entitiesUrl = this.wrapUrl("/query/entity/");
		this.categoriesUrl = this.entitiesUrl + "Category";
		this.productsUrl = this.entitiesUrl + "Product";
		this.lineItemUrl = this.entitiesUrl + "LineItem";
		this.orderUrl = this.entitiesUrl + "Order";
	}

	async createRepo(): Promise<Boolean> {
		//simple post of the since the body of the namespace if it is not present.
		//Note the body is quite large it is at the bottom of the file
		return await fetch(this.wrapUrl("/schema"), {
			body: schemaBody,
			method: "POST",
			headers: {
				Accept: "application/hocon",
				"Content-Type": "application/hocon"
			}
		}).then(x => x.status === 200);
	}

	async initializeState(): Promise<AppState> {
		let customer: Customer;
		const customerUrl = `${this.entitiesUrl}/Customer"`;
		const response = await fetch(
			//because of the lack of support for GET with a body via fetch, we have to pass this json to a query parameter named payload.
			//searching here by email of the Customer object and inclding the addresses for that customer in the response
			customerUrl +
				'?payload={ "-match": ["email", "=", "my@example.com"], "addresses": {}} ',
			{
				method: "GET",
				headers: jsonHeaders
			}
		)
			.then(x => x.json())
			.then(x => x as QueryResponse<Customer>);
		let address1: Address;
		if (response.data === undefined || response.data.length === 0) {
			address1 = new Address(
				"33 jones st",
				"apt 2",
				"Anytown",
				"84111",
				"USA"
			);
			customer = new Customer(
				"my@example.com",
				"John",
				"Smith"
			);
			customer.addresses = new Array<Address>(address1);
			const mutationResponse = await fetch(customerUrl, {
				method: "POST",
				headers: jsonHeaders,
				body: JSON.stringify(customer) //simple create case, just push an object graph into json and you have a simple create
			})
				.then(x => x.json())
				.then(x => x as MutationResponse);
			if (mutationResponse && mutationResponse.data) {
				customer.entityId =
					mutationResponse.data[0].entityId;
			} else {
				console.log(
					"unable to get mutation response back"
				);
				console.log(mutationResponse);
			}
		} else {
			customer = response.data[0];
		}
		let order: Order;
		const orderUrl = this.orderUrl;
		const orderResponse = await fetch(
			//because of the lack of support for GET with a body via fetch, we have to pass this json to a query parameter named payload.
			//the limit was set arbtrarily large as we are filtering by customer.email
			//this is a toy example and one would want to use a better query for this scenario.
			orderUrl +
				'?payload={ "-limit": 300, "lineItems": { "product": {}, "-include": ["entityId", "quantity"]}, "-match": ["customer.email", "=", "my@example.com"], "customer": {}}',
			{
				method: "GET",
				headers: jsonHeaders
			}
		)
			.then(x => x.json())
			.then(x => x as QueryResponse<Order>)
			.then(x => {
				if (x && x.data) {
					return x;
				} else {
					console.log("no order for " + x);
					return {
						data: new Array<Order>()
					};
				}
			})
			.then(x => x.data.filter((o: Order) => o.isOpen));
		if (orderResponse === undefined || orderResponse.length === 0) {
			const lineItems = new Array<LineItem>();
			if (customer.addresses === undefined) {
				throw new Error("no address is definied");
			}
			console.log(
				"no order successfully created with response " +
					orderResponse
			);
			order = new Order(lineItems, true);
			const orderCreatedResponse = await fetch(orderUrl, {
				method: "POST",
				headers: jsonHeaders,
				body: JSON.stringify(order) //one function call to create the body for an order
			})
				.then(x => x.json())
				.then(x => x as MutationResponse);
			console.log(orderCreatedResponse);
			if (orderCreatedResponse && orderCreatedResponse.data) {
				order.entityId =
					orderCreatedResponse.data[0].entityId;
			} else {
				console.log(
					"error creating order will not persist: " +
						orderCreatedResponse
				);
			}
		} else {
			console.log("retrieving latest order");
			order = orderResponse[orderResponse.length - 1];
		}
		//associate customer and order
		const updateQueryBody = `{
				"-match": [ "entityId", "=", "${order.entityId}"], 
				"customer": {
						"-match": ["entityId", "=", "${customer.entityId}"]
				},
				"deliveryAddress": {
						"-match": ["entityId", "=", "${customer.addresses[0].entityId}"]
				}
		}
		`;
		const updateOrderReponse = await fetch(orderUrl, {
			method: "PUT",
			headers: jsonHeaders,
			body: updateQueryBody
		})
			.then(x => x.json())
			.then(x => x as MutationResponse);
		console.log(updateOrderReponse);
		return {
			initialState: initialState,
			isOpen: false,
			ordersVisible: false,
			allProductCount: 0,
			customer: customer,
			order: order,
			products: new Array<Product>(),
			categories: new Array<String>(),
			searchTerm: "",
			categoryFilterValue: ""
		};
	}

	/**
	 * initializeCatalog creates the catalog
	 */
	async initializeCatalog(): Promise<Catalog> {
		//you'll note these are simple plain objects.
		const leafyCat = new Category("leafy", "stuff that is leafy");
		const lettuce = new Product(
			"romaine lettuce",
			"romaine_lettuce.jpg",
			"one of the more delicious lettuce leaves to put in your salad",
			89
		);
		leafyCat.products.push(lettuce);
		//root category
		const rootCat = new Category("roots", "root vegetables");
		const turnip = new Product(
			"turnip",
			"turnip.jpg",
			"tastier than you think",
			59
		);
		rootCat.products.push(turnip);
		const carrot = new Product(
			"carrot",
			"carrot.jpg",
			"last for a long time and good for the eyes",
			149
		);
		rootCat.products.push(carrot);
		const leek = new Product(
			"leek",
			"leek.jpg",
			"mild and pleasant, just clean them please.",
			450
		);
		rootCat.products.push(leek);
		const potato = new Product(
			"potato",
			"potato.jpg",
			"fry them up and enjoy",
			259
		);
		rootCat.products.push(potato);
		//fruit category
		const fruitCat = new Category(
			"fruits",
			"people think these are vegetables, but they're fruits"
		);
		const cucumber = new Product(
			"cucumbers",
			"cucumber.jpg",
			"crunchy and good to eat",
			150
		);
		fruitCat.products.push(cucumber);
		const eggplant = new Product(
			"eggplant",
			"eggplant.jpg",
			"need to cook it a lot",
			429
		);
		fruitCat.products.push(eggplant);
		const peas = new Product(
			"peas",
			"peas.jpg",
			"sweet and delicious",
			89
		);
		fruitCat.products.push(peas);
		const pumpkin = new Product(
			"pumpkin",
			"pumpkin.jpg",
			"good for pies",
			245
		);
		fruitCat.products.push(pumpkin);
		const tomato = new Product(
			"tomato",
			"tomato.jpg",
			"wonderful on nearly everything",
			245
		);
		fruitCat.products.push(tomato);
		//post it all at once
		const categories = new Array<Category>();
		categories.push(leafyCat);
		categories.push(rootCat);
		categories.push(fruitCat);
		//What follows is  great example of how to create several things in one operation and how easy this is to do.
		//NOTE: All relationsips in the object graph are linked by default.
		//this is managed by the server with a logged batch so careful how many one sends at a time.
		const categoryPost = JSON.stringify(categories);
		let response = await fetch(this.categoriesUrl, {
			method: "POST",
			body: categoryPost,
			headers: jsonHeaders
		});
		let categoryJsonResponse = await response.json();
		let categoryResponse = (await categoryJsonResponse) as CategoryMutationResponse;
		if (
			categoryResponse !== undefined &&
			categoryResponse.data !== undefined
		) {
			categoryResponse.data.forEach(
				(c: ParentMutation, i: number) => {
					categories[i].entityId = c.entityId;
					c.products.forEach(
						(m: Mutated, mi: number) => {
							categories[i].products[
								mi
							].entityId = m.entityId;
						}
					);
				}
			);
		} else {
			console.log(
				"failed to retrieve entityId for category, error is '" +
					categoryResponse +
					"'"
			);
		}
		return new Catalog(
			categories,
			categories.flatMap((x: Category) => x.products)
		);
	}

	/**
	 * search does a simple search in memory based on the search term existing inside of a product name
	 */

	async search(
		searchTerm: string,
		categoryFilter: string
	): Promise<Array<Product>> {
		const getPage = (
			continueId: string | undefined,
			products: Array<Product>
		): Promise<Array<Product>> => {
			//since the default 'fetch' client does not support passing in body for GET calls
			//we use the payload query string to send requests.
			//The default query is a "-match" : "all" which means all rows
			//The "-limit": 3 only gets 3 rows in a page. This is intentionally low to demonstrate paging.
			//The "category": {} means the category relationship is populated fully
			let fetchUrl =
				this.productsUrl +
				'?payload={  "-continue": true, "-limit": 3, "-match" : "all", "category": { }}';
			if (categoryFilter) {
				//if there is a categoryFilter provided the match changes to search on the relationship and not directly
				//on the parent object. Here "-match": ["category.name"], "=", "root"] will do an exact match on the "root" category name
				//note there is no enforced uniqueness on this type as of 0.2.0.
				fetchUrl =
					this.productsUrl +
					`?payload={ "-continue": true, "-limit": 3, "-match": ["category.name" , "=", "${categoryFilter}"], "category": {}}`;
			}
			if (continueId) {
				//if the continueId is present regardless of if there is a categoryFilter or not we can rely on the continueId to have our previous query parameters.
				fetchUrl = this.wrapUrl(
					"/query/continue/" + continueId
				);
			}

			const results = fetch(fetchUrl, {
				method: "GET",
				headers: jsonHeaders
			})
				.then(x => x.json())
				.then(x => x as QueryResponse<Product>)
				.then(x => {
					if (x && x.data) {
						//only return matches
						x.data
							.filter(
								(v: Product) =>
									v.name.indexOf(
										searchTerm
									) > -1
							)
							.forEach(x =>
								//add all matches to the products array
								products.push(x)
							);
						if (
							x.continue &&
							x.continue.continueId
						) {
							//if there is a continueId call this method again with the new continueId
							//and all newly found product matches.
							return getPage(
								x.continue
									.continueId,
								products
							);
						} else {
							//is there is no continueId present then we have the last page, go ahead and return it.
							return products;
						}
					}
					throw new Error(JSON.stringify(x));
				});
			return results;
		};
		//initial paging with an empty product array and no continueId
		return getPage(undefined, new Array<Product>());
	}

	/**
	 * buyNow closes the existing order so it will no longer be visible when return to the page.
	 */
	async buyNow(orderEntityId: string): Promise<QueryResponse<Mutated>> {
		//close the order now that it's been "bought"
		const setOrderToFalseBody = `{
										"-match": [ "entityId", "=", "${orderEntityId}" ],
										"isOpen": false
						}`;
		const response = await fetch(this.orderUrl, {
			method: "PUT",
			body: setOrderToFalseBody,
			headers: jsonHeaders
		})
			.then(x => {
				if (x.status === 200) {
					return x;
				} else {
					throw new Error(x.statusText);
				}
			})
			.then(x => x.json())
			.then(x => x as QueryResponse<Mutated>);
		return response;
	}

	/**
	 * empty the shopping cart of all line items.
	 */
	async clearCart(
		orderEntityId: string
	): Promise<QueryResponse<Mutated>> {
		//batch deletes all line items with the specified orderId
		const deleteAllLineItemsBody = `
						{ 
						"-match": [ "order.entityId", "=", "${orderEntityId}" ],
				   	"order": {}
						}
				`;
		const response = await fetch(this.lineItemUrl, {
			method: "DELETE",
			body: deleteAllLineItemsBody
		})
			.then(x => x.json())
			.then(x => x as QueryResponse<Mutated>);
		return response;
	}

	/**
	 * wrapUrl is a convience function to wrap the server url, the namespace, and any additional operation
	 */
	wrapUrl(url: string): string {
		return this.server + "/v1/api/" + this.ns + url;
	}

	/**
	 * addLineItem adds line items to an existing order
	 */
	async addLineItem(
		order: Order,
		lineItem: LineItem
	): Promise<Array<Mutated>> {
		//need to break out the body so we can easily map the
		//order and the product to the line item
		const lineItemBody = `
				{ 
				"quantity": ${lineItem.quantity},	
			   "order": {
						"-match": ["entityId", "=", "${order.entityId}" ]
				},
				"product": {
						"-match": ["entityId", "=", "${lineItem.product[0].entityId}"]
				}
			}
			`;
		const mutations = await fetch(this.lineItemUrl, {
			method: "POST",
			body: lineItemBody,
			headers: jsonHeaders
		})
			.then(x => x.json())
			.then(x => x as QueryResponse<Mutated>)
			.then(x => x.data);
		if (!mutations) {
			return new Array<Mutated>();
		}
		return mutations;
	}

	async deleteLineItem(
		entityId: string
	): Promise<QueryResponse<Mutated>> {
		//delete entity by entityId. Note there is no body.
		return await fetch(`${this.lineItemUrl}/${entityId}`, {
			method: "DELETE",
			headers: jsonHeaders
		})
			.then(x => x.json())
			.then(x => x as QueryResponse<Mutated>);
	}

	setRows(resultAction: (p: Array<Product>) => void): void {
		///setup a method for recursion so we can continue to page until we've gotten the full result set
		const getPage = (
			continueId: string | undefined,
			products: Array<Product>
		) => {
			//using the payload query string parameter because fetch does not support GET methods with a body
			//since this is the initial page load by default we are doing the "-match": "all" to retrieve all rows
			//3 rows at a time so we can demonstrate paging and how it works.
			let queryUrl =
				this.productsUrl +
				'?payload={ "-continue": true, "-limit": 3, "-match" : "all", "category": {}}';
			if (continueId) {
				//continue the query by the token provided by previous queries
				queryUrl = this.wrapUrl(
					`/query/continue/${continueId}`
				);
			}
			fetch(queryUrl, {
				method: "GET",
				headers: jsonHeaders
			})
				.then(x => x.json())
				.then(x => x as QueryResponse<Product>)
				.then(x => {
					x.data.forEach(x => products.push(x));
					//when there is a continueId present go ahead and page
					//otherwise just run the resultAction
					if (x.continue.continueId) {
						getPage(
							x.continue.continueId,
							products
						);
					} else {
						resultAction(products);
					}
				});
		};
		//start paging
		getPage(undefined, new Array<Product>());
	}
}

/**
 *	will create the namespace for you with this hocon if the veggies namespace is not present
 */
const schemaBody = `
entities {
    Customer {
        fields {
            email: string
            firstName: string
            lastName: string
            balance: float 
        }
        relations {
            addresses { type: Address, inverse: customers }
            orders { type: Order, inverse: customer }
        }
    }
    Address {
        fields {
            street1: string
            street2: string
            city: string
            postalCode: string
            country: string
        }
        relations {
            customers { type: Customer, inverse: addresses }
            orders { type: Order, inverse: deliveryAddress }
        }
    }
    Order {
        fields {
            time: int
            subtotal: int
            tax: int
            total: int
            isOpen: boolean
        }
        relations {
            customer { type: Customer, inverse: orders }
            deliveryAddress { type: Address, inverse: orders }
            lineItems { type: LineItem, inverse: order }
        }
    }
    LineItem {
      fields {
        quantity: int
        receiveBy: int
        savedPrice: int
      }
      relations {
        product { type: Product, inverse: lineItems }
        order {type: Order, inverse: lineItems }
      }
    }
    Product {
        fields {
            name: string
            desc: string
            photoUrl: string
            price: int
        }
        relations {
            category { type: Category, inverse: products }
            lineItems { type: LineItem, inverse: product }
        }
    }
    Category {
      fields {
        name: string
        desc: string
      }
      relations {
        products { type: Product, inverse: category }
      }
    }
}
queries: {
    Product: {
         productsBelowPriceRange {
            "-match": [ price, "<", price]
            "-include": [ entityId, name, desc, photoUrl ],
          }
    }
}
queryConditions: {
    Order: [
        ["lineItems.receiveBy", "="]
    ]
}
`;
