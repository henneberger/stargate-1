import React from 'react';
import { AppBar, Toolbar, Typography, Avatar } from "@material-ui/core";
import { makeStyles, createStyles, Theme } from '@material-ui/core/styles';
import { SearchBox } from './SearchBox';
import { deepOrange } from '@material-ui/core/colors';

export interface HeaderProps{
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
    backgroundColor: deepOrange[500],
  },
  }),
);

export const Header = (props: HeaderProps) => {
  const classes = useStyles();
  return (<AppBar position="fixed" className={props.appBarClass}>
    <Toolbar>
      <Avatar className={classes.orange}>U</Avatar>
      <Typography variant="h6" className={props.titleClass}>
        Veggies
      </Typography>
      <SearchBox
        updateSearch={props.updateSearch}
        searchBarClass={props.searchBarClass}
      />
    </Toolbar>
  </AppBar>);
}