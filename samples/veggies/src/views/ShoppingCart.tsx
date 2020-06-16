import React from "react";
import { Order, LineItem } from "../models/Stargate";
import ShoppingCartIcon from "@material-ui/icons/ShoppingCart";
import {
	List,
	ListItem,
	ListItemIcon,
	ListItemText,
	Divider,
} from "@material-ui/core";
import DeleteIcon from "@material-ui/icons/Delete";

export interface ShoppingCartProps {
	order: Order;
	updateLineItem: () => void;
	removeLineItem: (entityId: string | undefined) => void;
	buyNow: () => void;
}

export const ShoppingCart = (props: ShoppingCartProps) => {
	const order: Order = props.order;
	const lineItems = new Array<LineItem>();
	if (order && order.lineItems) {
		order.lineItems.forEach((x: LineItem) => lineItems.push(x));
	}
	const lineItemText = (l: LineItem) =>
		`${l.quantity} x ${l.product[0].name}`;
	let totalPrice = 0;
	if (order && order.lineItems) {
		order.lineItems.forEach(
			x => (totalPrice += x.product[0].price * x.quantity)
		);
	}
	if (totalPrice > 0) {
		totalPrice = totalPrice / 100.0;
	}
	const totalPriceText = "total price: $" + totalPrice.toFixed(2);
	return (
		<List>
			<ListItem>
				<ListItemIcon>
					<ShoppingCartIcon />
				</ListItemIcon>
			</ListItem>
			<Divider />
			{lineItems.map((lineItem: LineItem, i: number) => (
				<ListItem key={i}>
					<ListItemText
						primary={lineItemText(lineItem)}
					/>
					<ListItemIcon
						onClick={() =>
							props.removeLineItem(
								lineItem.entityId
							)
						}
					>
						<DeleteIcon />
					</ListItemIcon>
				</ListItem>
			))}
			<Divider />
			<ListItem>
				<ListItemText primary={totalPriceText} />
			</ListItem>
		</List>
	);
};
