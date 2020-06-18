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
	List,
	ListItem,
	ListItemText,
	ListItemProps,
	Drawer,
	Divider
} from "@material-ui/core";

export interface CategoryProps {
	categories: Array<String>;
	drawerClass: any;
	drawerPaperClass: any;
	drawerContainerClass: any;
	categoryFilter: (arg: String) => void;
	selectedCategory: string;
}

function ListItemLink(props: ListItemProps<"a", { button?: true }>) {
	return <ListItem button component="a" {...props} />;
}
export const CategoryList = (props: CategoryProps) => {
	return (
		<div id="categories">
			<Drawer
				variant="permanent"
				className={props.drawerClass}
				classes={{
					paper: props.drawerPaperClass
				}}
			>
				<div className={props.drawerContainerClass}>
					<List component="nav">
						<ListItem>
							<ListItemText>
								Veggie Types
							</ListItemText>
						</ListItem>
						<Divider />
						{props.categories.map(
							(
								category: String,
								i: number
							) => {
								return (
									<ListItemLink
										key={
											i
										}
										selected={
											category ===
											props.selectedCategory
										}
										onClick={() =>
											props.categoryFilter(
												category
											)
										}
									>
										<ListItemText
											primary={
												category
											}
										/>
									</ListItemLink>
								);
							}
						)}
						<Divider />
						<ListItemLink
							onClick={() =>
								props.categoryFilter(
									""
								)
							}
						>
							<ListItemText
								primary={"all"}
							/>
						</ListItemLink>
					</List>
				</div>
			</Drawer>
		</div>
	);
};
