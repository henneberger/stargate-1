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
import {
	GridList,
	GridListTile,
	GridListTileBar,
	IconButton,
	Button
} from "@material-ui/core";
import { Product } from "../models/Stargate";
import AddShoppingCartIcon from "@material-ui/icons/AddShoppingCart";
import { grey } from "@material-ui/core/colors";

interface ProductProps {
	products: Array<Product>;
	allProductsCount: number;
	initializeCatalog: () => void;
	addLineItem: (eventId: string | undefined) => void;
}

export const ProductList = (props: ProductProps) => {
	if (props.allProductsCount === 0) {
		return (
			<Button
				color="primary"
				variant="contained"
				onClick={props.initializeCatalog}
			>
				Initialize Catalog
			</Button>
		);
	}
	if (props.products.length > 0) {
		console.log(props.products);
		return (
			<GridList cellHeight={120} className="producs" cols={3}>
				{props.products.map((product, i) => (
					<GridListTile key={i} cols={1}>
						<img
							src={product.photoUrl}
							alt={product.name}
						/>
						<GridListTileBar
							title={product.name}
							subtitle={
								<span>
									$
									{(
										product.price /
										100
									).toFixed(
										2
									)}
								</span>
							}
							actionIcon={
								<IconButton
									onClick={() =>
										props.addLineItem(
											product.entityId
										)
									}
									aria-label={`add ${product.name}`}
								>
									<AddShoppingCartIcon
										style={{
											color:
												grey[200]
										}}
									/>
								</IconButton>
							}
							actionPosition="left"
						/>
					</GridListTile>
				))}
			</GridList>
		);
	} else {
		return <div>no results</div>;
	}
};
