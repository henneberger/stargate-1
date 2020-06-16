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
								primary={
									"all"
								}
							/>
						</ListItemLink>
					</List>
				</div>
			</Drawer>
		</div>
	);
};
