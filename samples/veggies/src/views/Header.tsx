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
import { AppBar, Toolbar, Typography, Avatar } from "@material-ui/core";
import { makeStyles, createStyles, Theme } from "@material-ui/core/styles";
import { SearchBox } from "./SearchBox";
import { deepOrange } from "@material-ui/core/colors";

export interface HeaderProps {
	updateSearch(ev: React.ChangeEvent<HTMLInputElement>): void;
	appBarClass: any;
	titleClass: any;
	menuButtonClass: any;
	searchBarClass: any;
}

const useStyles = makeStyles((theme: Theme) =>
	createStyles({
		orange: {
			color: theme.palette.getContrastText(deepOrange[500]),
			backgroundColor: deepOrange[500]
		}
	})
);

export const Header = (props: HeaderProps) => {
	const classes = useStyles();
	return (
		<AppBar position="fixed" className={props.appBarClass}>
			<Toolbar>
				<Avatar className={classes.orange}>U</Avatar>
				<Typography
					variant="h6"
					className={props.titleClass}
				>
					Veggies
				</Typography>
				<SearchBox
					updateSearch={props.updateSearch}
					searchBarClass={props.searchBarClass}
				/>
			</Toolbar>
		</AppBar>
	);
};
