import React from "react";
import { TextField, Grid } from "@material-ui/core";

export interface SearchBoxProps {
  updateSearch(ev: React.ChangeEvent<HTMLInputElement>): void;
  searchBarClass: any; 
}

export const SearchBox = (props: SearchBoxProps) => (
  <Grid container id="searchBox">
    <Grid item xs={2}></Grid>
    <Grid item xs={8}>
      <TextField
        id="filled-search"
        label="Search"
        type="search"
        variant="filled"
        fullWidth
        className={props.searchBarClass}
        onChange={props.updateSearch}
      ></TextField>
    </Grid>
  </Grid>
);
