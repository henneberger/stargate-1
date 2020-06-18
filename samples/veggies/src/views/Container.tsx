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
import React from "react";
import { CategoryList } from "./CategoryList";
import { Header } from "./Header";
import { ProductList } from "./ProductList";
import { ShoppingCart } from "./ShoppingCart";
import { Customer, Order, Product } from "../models/Stargate";
import {
	createStyles,
	makeStyles,
	Theme,
	ThemeProvider
} from "@material-ui/core/styles";
import CssBaseline from "@material-ui/core/CssBaseline";
import ShoppingBasketIcon from "@material-ui/icons/ShoppingBasket";
import HelpIcon from "@material-ui/icons/Help";
import RemoveShoppingCartIcon from "@material-ui/icons/RemoveShoppingCart";
import {
	Grid,
	BottomNavigation,
	BottomNavigationAction,
	Dialog,
	DialogTitle,
	Button,
	ButtonGroup,
	Drawer
} from "@material-ui/core";
import { createMuiTheme } from "@material-ui/core/styles";
import green from "@material-ui/core/colors/green";

const drawerWidth = 200;
const useStyles = makeStyles((theme: Theme) =>
	createStyles({
		root: {
			// display: "flex",
		},
		appBar: {
			zIndex: theme.zIndex.drawer + 1
		},

		drawer: {
			width: drawerWidth,
			flexShrink: 0
		},
		drawerPaper: {
			width: drawerWidth
		},
		drawerContainer: {
			marginTop: 100,
			overflow: "auto"
		},
		content: {
			marginTop: 100,
			minHeight: 500
			//flexGrow: 1,
			//padding: theme.spacing(10),
		},
		menuButton: {
			marginRight: theme.spacing(2)
		},
		searchBar: {
			backgroundColor: "white"
		},
		title: {
			flexGrow: 1
		},
		footer: {
			zIndex: theme.zIndex.drawer + 1,
			height: "2.5rem",
			clear: "both",
			width: "100%",
			position: "absolute",
			bottom: 0,
			textAlign: "center"
		}
	})
);
export interface ContainerProps {
	isOpen: boolean;
	customer: Customer;
	products: Array<Product>;
	allProductCount: number;
	categories: Array<String>;
	searchTerm: string;
	order: Order;
	updateSearch(ev: React.ChangeEvent<HTMLInputElement>): void;
	initializeCatalog: () => void;
	addLineItem: (entityId: string | undefined) => void;
	updateLineItem: () => void;
	removeLineItem: (entityId: string | undefined) => void;
	buyNow: () => void;
	showOrders: () => void;
	emptyCart: () => void;
	categoryFilterValue: string;
	categoryFilter: (category: String) => void;
	showDialog: (isOpen: boolean) => void;
}

const theme = createMuiTheme({
	palette: {
		primary: green
	}
});

export const Container = (props: ContainerProps) => {
	const classes = useStyles();
	return (
		<ThemeProvider theme={theme}>
			<div className={classes.root}>
				<CssBaseline />
				<div>
					<Grid container spacing={1}>
						<Grid item xs={12}>
							<Header
								updateSearch={
									props.updateSearch
								}
								titleClass={
									classes.title
								}
								menuButtonClass={
									classes.menuButton
								}
								appBarClass={
									classes.appBar
								}
								searchBarClass={
									classes.searchBar
								}
							/>
						</Grid>
						<Grid item xs={2}>
							<CategoryList
								categoryFilter={
									props.categoryFilter
								}
								categories={
									props.categories
								}
								drawerClass={
									classes.drawer
								}
								drawerPaperClass={
									classes.drawerPaper
								}
								drawerContainerClass={
									classes.drawerContainer
								}
								selectedCategory={
									props.categoryFilterValue
								}
							/>
						</Grid>
						<Grid
							item
							xs={8}
							className={
								classes.content
							}
						>
							<ProductList
								allProductsCount={
									props.allProductCount
								}
								initializeCatalog={
									props.initializeCatalog
								}
								products={
									props.products
								}
								addLineItem={
									props.addLineItem
								}
							/>
						</Grid>
						<Grid item xs={2}>
							<Drawer
								className={
									classes.drawer
								}
								variant="permanent"
								classes={{
									paper:
										classes.drawerPaper
								}}
								anchor="right"
							>
								<div
									className={
										classes.drawerContainer
									}
								>
									<ShoppingCart
										order={
											props.order
										}
										buyNow={
											props.buyNow
										}
										updateLineItem={
											props.updateLineItem
										}
										removeLineItem={
											props.removeLineItem
										}
									/>
								</div>
							</Drawer>
						</Grid>
						<Grid
							item
							xs={12}
							className={
								classes.footer
							}
						>
							<BottomNavigation
								showLabels
							>
								<BottomNavigationAction
									label="Buy Now"
									onClick={() =>
										props.showDialog(
											true
										)
									}
									icon={
										<ShoppingBasketIcon />
									}
								/>
								<BottomNavigationAction
									label="Empty Cart"
									onClick={
										props.emptyCart
									}
									icon={
										<RemoveShoppingCartIcon />
									}
								/>
								<BottomNavigationAction
									onClick={() =>
										window.location.replace(
											"https://www.github.com/datastax/stargate"
										)
									}
									label="about"
									icon={
										<HelpIcon />
									}
								/>
							</BottomNavigation>
							<Dialog
								onClose={() =>
									props.showDialog(
										false
									)
								}
								aria-labelledby="simple-dialog-title"
								open={
									props.isOpen
								}
							>
								<DialogTitle id="simple-dialog-title">
									Buy now
								</DialogTitle>
								<ShoppingCart
									order={
										props.order
									}
									buyNow={
										props.buyNow
									}
									updateLineItem={
										props.updateLineItem
									}
									removeLineItem={
										props.removeLineItem
									}
								/>

								<ButtonGroup>
									<Button
										variant="contained"
										color="primary"
										onClick={
											props.buyNow
										}
									>
										Complete
										Order
									</Button>
									<Button
										onClick={() =>
											props.showDialog(
												false
											)
										}
										variant="contained"
										color="secondary"
									>
										Cancel
									</Button>
								</ButtonGroup>
							</Dialog>
						</Grid>
					</Grid>
				</div>
			</div>
		</ThemeProvider>
	);
};
